package com.phatjam98.elasticprotos.migration

import com.phatjam98.elasticprotos.engine.IndexOptions
import com.phatjam98.elasticprotos.testkit.InMemorySearchEngine
import java.time.Duration
import java.time.Instant
import spock.lang.Specification

/** The per-alias lock: one document per alias in the lock index. */
class AliasLockSpec extends Specification {

  static final String LOCKS = 'elastic-protos-locks'

  RacingEngine engine = new RacingEngine()

  AliasLock lock(Duration ttl = Duration.ofMinutes(2)) {
    new AliasLock(engine, LOCKS, ttl, { 0 })
  }

  void writeLock(String alias, String holder, Instant expires) {
    if (!engine.indexExists(LOCKS)) {
      engine.createIndex(LOCKS, [:], [:], [] as Set)
    }
    engine.index(LOCKS, alias, [alias: alias, holder: holder, action: 'CREATE', hash: 'h',
                                acquired_at: Instant.now().minusSeconds(600).toString(), expires_at: expires.toString()],
        IndexOptions.refreshed())
  }

  def "a lock is a document in a strictly mapped one-shard index, deleted on release"() {
    given:
    def locks = lock()

    when:
    def lease = locks.acquire('documents', 'REINDEX', 'abc', Duration.ZERO)
    def doc = engine.documents(LOCKS).documents

    then:
    locks.holder() ==~ /.+\/\d+\/[0-9a-f]{8}/
    locks.lockIndex() == LOCKS
    lease.alias() == 'documents'
    doc.alias == 'documents'
    doc.holder == locks.holder()
    doc.action == 'REINDEX'
    doc.hash == 'abc'
    Instant.parse(doc.expires_at).isAfter(Instant.parse(doc.acquired_at).plusSeconds(119))
    engine.getMappings(LOCKS).dynamic == 'strict'
    engine.getMappings(LOCKS).properties.keySet() == ['alias', 'holder', 'action', 'hash', 'acquired_at', 'expires_at'] as Set
    engine.getSettings(LOCKS).index.number_of_shards == '1'
    engine.getSettings(LOCKS).index.number_of_replicas == '0'

    when:
    lease.close()
    lease.close()

    then:
    engine.documents(LOCKS).isEmpty()
    engine.log().count { it == "delete ${LOCKS}/documents".toString() } == 1
  }

  def "the lock index is created once, and again when it disappears"() {
    given:
    def locks = lock()

    when:
    locks.acquire('a', 'CREATE', 'h', Duration.ZERO).close()
    locks.acquire('b', 'CREATE', 'h', Duration.ZERO).close()

    then:
    engine.log().count { it.startsWith("createIndex ${LOCKS}") } == 1

    when: 'someone deletes the lock index'
    engine.deleteIndex(LOCKS)
    locks.acquire('c', 'CREATE', 'h', Duration.ZERO).close()

    then:
    engine.log().count { it.startsWith("createIndex ${LOCKS}") } == 2
  }

  def "another process creating the lock index first is fine"() {
    given:
    engine.beforeCreate = { engine.beforeCreate = null; engine.createIndex(LOCKS, [:], [:], [] as Set) }

    expect:
    lock().acquire('a', 'CREATE', 'h', Duration.ZERO) != null
  }

  def "a live lock held by another process fails at once with a zero wait, and after the wait otherwise"() {
    given:
    lock().acquire('documents', 'REINDEX', 'abc', Duration.ZERO)
    def other = lock()

    when:
    other.acquire('documents', 'CREATE', 'h', Duration.ZERO)

    then:
    def e = thrown(LockException)
    !e.lost()
    e.holder() != other.holder()
    e.message.startsWith('the migration lock of documents is held by ')
    e.message.contains('(REINDEX, expires at ')

    when:
    long started = System.nanoTime()
    other.acquire('documents', 'CREATE', 'h', Duration.ofMillis(300))

    then:
    thrown(LockException)
    System.nanoTime() - started >= 300_000_000L
  }

  def "a waiting process takes the lock once the holder releases it"() {
    given:
    def lease = lock().acquire('documents', 'REINDEX', 'abc', Duration.ZERO)
    Thread.start { sleep(300); lease.close() }

    when:
    def taken = lock().acquire('documents', 'CREATE', 'h', Duration.ofSeconds(10))

    then:
    taken != null
    engine.documents(LOCKS).documents.action == 'CREATE'
  }

  def "an expired lock is taken over by exactly one of two takers"() {
    given:
    writeLock('documents', 'dead/1/deadbeef', Instant.now().minusSeconds(120))
    def a = lock()
    def b = lock()
    AliasLock.Lease wonByB = null
    engine.beforeDelete = { engine.beforeDelete = null; wonByB = b.acquire('documents', 'CREATE', 'h', Duration.ZERO) }

    when: 'a reads the expired lock, then b takes it over before a deletes it'
    a.acquire('documents', 'CREATE', 'h', Duration.ZERO)

    then: 'a conditional delete on the stale sequence number loses, and a finds b holding the lock'
    def e = thrown(LockException)
    e.holder() == b.holder()
    wonByB != null
    engine.documents(LOCKS).documents.holder == b.holder()
  }

  def "a lock without a readable expiry is treated as expired"() {
    given:
    engine.createIndex(LOCKS, [:], [:], [] as Set)
    engine.index(LOCKS, 'documents', [alias: 'documents', holder: 'x'], IndexOptions.refreshed())

    expect:
    lock().acquire('documents', 'CREATE', 'h', Duration.ZERO) != null
  }

  def "the heartbeat moves the expiry on, and finds a deleted or taken-over lock lost"() {
    given:
    def locks = lock()
    def lease = locks.acquire('documents', 'REINDEX', 'abc', Duration.ZERO)
    String first = engine.documents(LOCKS).documents.expires_at
    sleep(5)

    when:
    lease.heartbeat()

    then:
    Instant.parse(engine.documents(LOCKS).documents.expires_at).isAfter(Instant.parse(first))
    lease.check()

    when: 'the lock document is taken over underneath the lease'
    writeLock('documents', 'other/2/cafebabe', Instant.now().plusSeconds(60))
    lease.heartbeat()
    lease.check()

    then:
    def e = thrown(LockException)
    e.lost()
    e.message.startsWith('the migration lock of documents was lost')

    when: 'a lost lease is closed'
    lease.close()
    lease.heartbeat()

    then: 'the other holder keeps its lock'
    engine.documents(LOCKS).documents.holder == 'other/2/cafebabe'
  }

  def "a deleted lock document loses the lease"() {
    given:
    def lease = lock().acquire('documents', 'REINDEX', 'abc', Duration.ZERO)
    engine.delete(LOCKS, 'documents')

    when:
    lease.heartbeat()
    lease.check()

    then:
    thrown(LockException)
  }

  def "the heartbeat runs on its own every third of the TTL"() {
    given:
    def lease = lock(Duration.ofSeconds(1)).acquire('documents', 'REINDEX', 'abc', Duration.ZERO)
    long seqNo = engine.get(LOCKS, 'documents').get().seqNo()

    expect:
    new spock.util.concurrent.PollingConditions(timeout: 10).eventually {
      assert engine.get(LOCKS, 'documents').get().seqNo() > seqNo
    }

    cleanup:
    lease?.close()
  }

  def "a lease expires locally two thirds of the TTL after its last successful write"() {
    given:
    def clock = new IndexMigratorLockSpec.MutableClock()
    def locks = new AliasLock(engine, LOCKS, Duration.ofSeconds(90), { 0 }, clock)
    def lease = locks.acquire('documents', 'REINDEX', 'abc', Duration.ZERO)

    when: 'just under two thirds of the TTL'
    clock.advance(Duration.ofSeconds(59))
    lease.check()

    then:
    notThrown(LockException)

    when: 'a renewal fails with a transient error: retried, not lost, and the lease does not move'
    engine.failIndex = true
    lease.heartbeat()
    clock.advance(Duration.ofSeconds(2))
    lease.check()

    then:
    def e = thrown(LockException)
    e.lost()
    e.message.contains('no renewal succeeded since')
  }

  def "a successful renewal moves the local expiry on"() {
    given:
    def clock = new IndexMigratorLockSpec.MutableClock()
    def lease = new AliasLock(engine, LOCKS, Duration.ofSeconds(90), { 0 }, clock)
        .acquire('documents', 'REINDEX', 'abc', Duration.ZERO)

    when:
    clock.advance(Duration.ofSeconds(50))
    lease.heartbeat()
    clock.advance(Duration.ofSeconds(50))
    lease.check()

    then:
    notThrown(LockException)
  }

  def "an expired lock is taken over only a third of the TTL after it expired"() {
    given:
    writeLock('documents', 'skewed/1/deadbeef', Instant.now().minusSeconds(10))

    when:
    lock().acquire('documents', 'CREATE', 'h', Duration.ZERO)

    then:
    def e = thrown(LockException)
    e.holder() == 'skewed/1/deadbeef'
  }

  def "an acquire response without sequence numbers is refused"() {
    given:
    engine.noSeqNo = true

    when:
    lock().acquire('documents', 'CREATE', 'h', Duration.ZERO)

    then:
    def e = thrown(IllegalStateException)
    e.message.contains('did not return _seq_no and _primary_term')
  }

  def "engine errors other than conflicts are reported"() {
    given:
    engine.failIndex = true

    when:
    lock().acquire('documents', 'CREATE', 'h', Duration.ZERO)

    then:
    thrown(com.phatjam98.elasticprotos.engine.EngineException)
  }

  /** Runs a hook before the next create or conditional delete, to interleave two processes. */
  static class RacingEngine extends InMemorySearchEngine {
    Closure beforeDelete
    Closure beforeCreate
    boolean failIndex
    boolean noSeqNo

    @Override
    boolean delete(String index, String id, IndexOptions options) {
      beforeDelete?.call()
      super.delete(index, id, options)
    }

    @Override
    void createIndex(String name, Map<String, Object> settings, Map<String, Object> mappings, Set<String> aliases) {
      beforeCreate?.call()
      super.createIndex(name, settings, mappings, aliases)
    }

    @Override
    com.phatjam98.elasticprotos.engine.IndexResult index(String index, String id, Map<String, Object> document, IndexOptions options) {
      if (failIndex) {
        throw new com.phatjam98.elasticprotos.engine.EngineException(
            com.phatjam98.elasticprotos.engine.EngineException.Kind.TRANSIENT, 503, 'busy')
      }
      def result = super.index(index, id, document, options)
      noSeqNo ? new com.phatjam98.elasticprotos.engine.IndexResult(result.index(), result.id(), result.result(),
          result.version()) : result
    }
  }
}
