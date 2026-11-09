package com.phatjam98.elasticprotos.migration

import com.phatjam98.elasticprotos.engine.IndexOptions
import com.phatjam98.elasticprotos.mapping.IndexNamingStrategy
import com.phatjam98.elasticprotos.mapping.MappingOverrides
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.migration.MigrationListener.Phase
import com.phatjam98.elasticprotos.proto.Bool3
import com.phatjam98.elasticprotos.proto.EsType
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.testkit.InMemorySearchEngine
import java.time.Duration
import java.time.Instant
import spock.lang.Specification

/** apply and rollback take the per-alias lock, plan again under it, and stop when it is lost. */
class IndexMigratorLockSpec extends Specification {

  static final String LOCKS = 'elastic-protos-locks'
  static final ProtoMapper DEFAULT = ProtoMapper.withDefaults()
  static final ProtoMapper NO_RELEVANCE = ProtoMapper.builder()
      .overrides(MappingOverrides.builder().ignorePath('relevance').build()).build()
  static final ProtoMapper BODY_KEYWORD = ProtoMapper.builder()
      .overrides(MappingOverrides.builder().forPath('body') { it.setKeywordSubfield(Bool3.TRUE) }.build()).build()
  static final ProtoMapper TITLE_KEYWORD = ProtoMapper.builder()
      .overrides(MappingOverrides.builder().forPath('title') { it.setType(EsType.KEYWORD) }.build()).build()
  static final MigrationOptions LOCKED = MigrationOptions.standard().withSwapGrace(Duration.ZERO)

  InMemorySearchEngine engine = new InMemorySearchEngine()
  Map<Phase, Closure> on = [:]

  IndexMigrator migrator(ProtoMapper mapper = DEFAULT, MigrationOptions options = LOCKED) {
    IndexMigrator.builder().engine(engine).mapper(mapper).options(options)
        .listener({ alias, phase -> on[phase]?.call() } as MigrationListener).build()
  }

  void seed(ProtoMapper mapper = DEFAULT) {
    migrator(mapper, LOCKED.withLock(false)).bootstrap(Document)
    3.times { engine.index('documents', "d${it}", [id: "d${it}".toString(), title: 't', body: 'b'], IndexOptions.defaults()) }
    engine.clearLog()
  }

  List<String> lockOps() {
    engine.log().findAll { it.contains(LOCKS) }
  }

  def "#action runs between taking and releasing the alias lock"() {
    given:
    if (before != null) {
      seed(before)
    }

    when:
    def result = migrator(after).bootstrap(Document)
    def log = engine.log()
    int taken = log.indexOf("index ${LOCKS}/documents".toString())
    int released = log.indexOf("delete ${LOCKS}/documents".toString())
    int firstChange = log.findIndexOf { it ==~ /(createIndex documents|putMappings|updateByQuery|reindex|updateAliases).*/ }

    then:
    result.ok()
    result.outcomes()[0].plan().action() == action
    taken >= 0
    taken < firstChange
    released == log.size() - 1
    engine.documents(LOCKS).isEmpty()

    where:
    action             | before       | after
    Action.CREATE      | null         | DEFAULT
    Action.PUT_MAPPING | NO_RELEVANCE | DEFAULT
    Action.BACKFILL    | DEFAULT      | BODY_KEYWORD
    Action.REINDEX     | DEFAULT      | TITLE_KEYWORD
  }

  def "an up-to-date index takes no lock"() {
    given:
    seed()

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.SKIPPED
    lockOps().isEmpty()
  }

  def "a stale write block is cleared under the lock"() {
    given:
    seed()
    engine.setWriteBlock(DEFAULT.map(Document).name(), true)
    engine.clearLog()

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.APPLIED
    !lockOps().isEmpty()
  }

  def "with the lock off nothing touches the lock index"() {
    when:
    migrator(DEFAULT, LOCKED.withLock(false)).bootstrap(Document)
    seed()
    migrator(TITLE_KEYWORD, LOCKED.withLock(false)).bootstrap(Document)

    then:
    !engine.log().any { it.contains(LOCKS) }
    !engine.indexExists(LOCKS)
  }

  def "the item is planned again under the lock, so work another process finished is a NO_OP"() {
    given: 'a plan made before another process created the index'
    def plan = migrator().plan(Document)
    migrator(DEFAULT, LOCKED.withLock(false)).bootstrap(Document)
    engine.clearLog()

    when:
    def result = migrator().apply(plan)

    then:
    plan.items()[0].action() == Action.CREATE
    result.ok()
    result.outcomes()[0].plan().action() == Action.NO_OP
    result.outcomes()[0].status() == MigrationResult.Status.SKIPPED
    engine.log().findAll { it.startsWith('createIndex') } == ["createIndex ${LOCKS}".toString()]
  }

  def "a waiting process re-plans to NO_OP once the holder finished"() {
    given: 'another process holds the lock while it creates the index'
    def plan = migrator().plan(Document)
    def other = new AliasLock(engine, LOCKS, Duration.ofMinutes(2), { 0 })
    def lease = other.acquire('documents', 'CREATE', 'h', Duration.ZERO)
    migrator(DEFAULT, LOCKED.withLock(false)).bootstrap(Document)
    Thread.start { sleep(300); lease.close() }

    when:
    def result = migrator().apply(plan)

    then:
    result.ok()
    result.outcomes()[0].plan().action() == Action.NO_OP
  }

  def "a lock held past lockWait makes the item LOCKED and stops FAIL_FAST"() {
    given:
    new AliasLock(engine, LOCKS, Duration.ofMinutes(2), { 0 }).acquire('documents', 'REINDEX', 'h', Duration.ZERO)

    when:
    def result = migrator(DEFAULT, LOCKED.withLockWait(Duration.ZERO)).bootstrap(Document, Firm)

    then:
    !result.ok()
    result.outcomes().size() == 1
    result.outcomes()[0].status() == MigrationResult.Status.LOCKED
    result.outcomes()[0].error() instanceof LockException
    result.problems().size() == 1
    !engine.indexExists('documents')
  }

  def "with CONTINUE a locked alias does not stop the others"() {
    given:
    new AliasLock(engine, LOCKS, Duration.ofMinutes(2), { 0 }).acquire('documents', 'REINDEX', 'h', Duration.ZERO)

    when:
    def result = migrator(DEFAULT, LOCKED.withLockWait(Duration.ZERO).withFailure(FailurePolicy.CONTINUE))
        .bootstrap(Document, Firm)

    then:
    result.outcomes()*.status() == [MigrationResult.Status.LOCKED, MigrationResult.Status.APPLIED]
  }

  def "an expired lock left by a dead process is taken over"() {
    given:
    engine.createIndex(LOCKS, [:], [:], [] as Set)
    engine.index(LOCKS, 'documents', [alias: 'documents', holder: 'dead/1/deadbeef', action: 'REINDEX', hash: 'h',
                                      acquired_at: '2026-01-01T00:00:00Z', expires_at: Instant.now().minusSeconds(1).toString()],
        IndexOptions.refreshed())

    expect:
    migrator(DEFAULT, LOCKED.withLockWait(Duration.ZERO)).bootstrap(Document).ok()
  }

  def "the lock index follows the naming strategy"() {
    given:
    def prefixed = ProtoMapper.builder().naming(IndexNamingStrategy.prefixed('dev')).build()

    when:
    migrator(prefixed).bootstrap(Document)

    then:
    engine.indexExists('dev-elastic-protos-locks')
  }

  def "a failure to take the lock fails the item"() {
    given:
    engine.createIndex(LOCKS, [:], [:], [] as Set)
    engine.setWriteBlock(LOCKS, true)

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
  }

  def "a lease lost before the alias swap stops the reindex without cleanup, and the next run finishes it"() {
    given:
    seed()
    String v1 = DEFAULT.map(Document).name()
    String v2 = TITLE_KEYWORD.map(Document).name()
    on[Phase.SNAPSHOT] = {
      // the lock is taken over while the copy ran; the next heartbeat notices
      engine.index(LOCKS, 'documents', [alias: 'documents', holder: 'other/2/cafebabe', action: 'REINDEX', hash: 'h',
                                        acquired_at: Instant.now().toString(), expires_at: Instant.now().minusSeconds(1).toString()],
          IndexOptions.refreshed())
      sleep(800)
    }

    when:
    def result = migrator(TITLE_KEYWORD, LOCKED.withLockTtl(Duration.ofSeconds(1))).bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
    result.outcomes()[0].error() instanceof LockException
    (result.outcomes()[0].error() as LockException).lost()
    result.outcomes()[0].index() == v1
    engine.indices()[v1] == ['documents'] as Set
    !engine.isWriteBlocked(v1)
    engine.indexExists(v2)
    engine.getMappings(v2)._meta.elastic_protos.pending.phase == 'swapping'

    when: 'the next run takes over the expired lock and resumes'
    on.clear()
    def next = migrator(TITLE_KEYWORD).bootstrap(Document)

    then:
    next.ok()
    engine.indices()[v2] == ['documents'] as Set
    engine.getMappings(v2)._meta.elastic_protos.pending == null
    engine.documents(LOCKS).isEmpty()
  }

  def "a lease lost after the swap fails the item, and the next run resumes the reconcile"() {
    given:
    seed()
    String v2 = TITLE_KEYWORD.map(Document).name()
    on[Phase.SWAPPED] = {
      engine.delete(LOCKS, 'documents')
      sleep(800)
    }

    when:
    def result = migrator(TITLE_KEYWORD, LOCKED.withLockTtl(Duration.ofSeconds(1))).bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
    result.outcomes()[0].index() == v2
    engine.getMappings(v2)._meta.elastic_protos.pending.phase == 'swapping'

    when:
    on.clear()
    def next = migrator(TITLE_KEYWORD).bootstrap(Document)

    then:
    next.ok()
    next.outcomes()[0].plan().resume().swapped()
    engine.getMappings(v2)._meta.elastic_protos.pending == null
  }

  def "rollback takes the lock too"() {
    given:
    seed()
    def plan = migrator(TITLE_KEYWORD).plan(Document)
    migrator(TITLE_KEYWORD).apply(plan)
    engine.clearLog()

    when:
    def outcome = migrator(TITLE_KEYWORD).rollback(plan.items()[0], plan.items()[0].currentIndex(), RollbackMode.ALIAS_ONLY)

    then:
    outcome.ok()
    engine.log().first() ==~ /(indexExists|index) ${LOCKS}.*/
    engine.log().last() == "delete ${LOCKS}/documents".toString()

    when: 'another process holds the lock'
    new AliasLock(engine, LOCKS, Duration.ofMinutes(2), { 0 }).acquire('documents', 'ROLLBACK', 'h', Duration.ZERO)
    def locked = migrator(TITLE_KEYWORD, LOCKED.withLockWait(Duration.ZERO)).rollback(plan.items()[0])

    then:
    locked.status() == MigrationResult.Status.LOCKED
  }

  def "a rollback whose lease is lost stops"() {
    given:
    seed()
    def plan = migrator(TITLE_KEYWORD).plan(Document)
    migrator(TITLE_KEYWORD).apply(plan)
    on[Phase.COPIED] = { engine.delete(LOCKS, 'documents'); sleep(800) }

    when:
    def outcome = migrator(TITLE_KEYWORD, LOCKED.withLockTtl(Duration.ofSeconds(1))).rollback(plan.items()[0])

    then:
    outcome.status() == MigrationResult.Status.FAILED
    outcome.error() instanceof LockException
  }

  def "a lock that cannot be taken for a rollback fails it"() {
    given:
    seed()
    def plan = migrator(TITLE_KEYWORD).plan(Document)
    migrator(TITLE_KEYWORD).apply(plan)
    engine.setWriteBlock(LOCKS, true)

    expect:
    migrator(TITLE_KEYWORD).rollback(plan.items()[0]).status() == MigrationResult.Status.FAILED
  }
}
