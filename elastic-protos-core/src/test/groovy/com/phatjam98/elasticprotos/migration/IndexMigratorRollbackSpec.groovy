package com.phatjam98.elasticprotos.migration

import com.phatjam98.elasticprotos.engine.EngineException
import com.phatjam98.elasticprotos.engine.IndexOptions
import com.phatjam98.elasticprotos.mapping.MappingOverrides
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.migration.MigrationListener.Phase
import com.phatjam98.elasticprotos.proto.EsType
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.testkit.InMemorySearchEngine
import java.time.Duration
import spock.lang.Specification

/** Rolling a reindex back: copying the writes made since, or only moving the alias. */
class IndexMigratorRollbackSpec extends Specification {

  static final ProtoMapper DEFAULT = ProtoMapper.withDefaults()
  static final ProtoMapper TITLE_KEYWORD = ProtoMapper.builder()
      .overrides(MappingOverrides.builder().forPath('title') { it.setType(EsType.KEYWORD) }.build())
      .build()
  static final String V1 = DEFAULT.map(Document).name()
  static final String V2 = TITLE_KEYWORD.map(Document).name()
  static final MigrationOptions ZD = MigrationOptions.standard().withSwapGrace(Duration.ZERO).withLock(false)

  InMemorySearchEngine engine = new InMemorySearchEngine()
  Map<Phase, Closure> on = [:]
  IndexPlan plan

  IndexMigrator migrator(MigrationOptions options = ZD) {
    IndexMigrator.builder().engine(engine).mapper(TITLE_KEYWORD).options(options)
        .listener({ alias, phase -> on[phase]?.call() } as MigrationListener).build()
  }

  def setup() {
    IndexMigrator.builder().engine(engine).mapper(DEFAULT).options(MigrationOptions.standard().withLock(false)).build().bootstrap(Document)
    4.times { put("d${it}", "Title ${it}") }
  }

  void migrate(MigrationOptions options = ZD) {
    def migration = migrator(options).plan(Document)
    plan = migration.items()[0]
    assert migrator(options).apply(migration).ok()
    on.clear()
  }

  void put(String id, String title) {
    engine.index('documents', id, [id: id, title: title, body: 'b'], IndexOptions.defaults())
  }

  Map<String, String> titles(String index) {
    engine.documents(index).collectEntries { id, doc -> [(id): doc.title] }
  }

  def "a reverse-delta rollback copies the writes made after the reindex back"() {
    given:
    migrate()
    put('d0', 'after the reindex')
    put('n1', 'new')
    engine.delete('documents', 'd1')

    when:
    def outcome = migrator().rollback(plan)

    then:
    outcome.status() == MigrationResult.Status.APPLIED
    outcome.index() == V1
    outcome.retainedIndices() == [V2]
    outcome.reconcile() != null
    titles(V1) == [d0: 'after the reindex', d2: 'Title 2', d3: 'Title 3', n1: 'new']
    engine.indices() == [(V1): ['documents'] as Set, (V2): [] as Set]
    !engine.isWriteBlocked(V1)
    engine.isWriteBlocked(V2)
    engine.getMappings(V1)._meta.elastic_protos.pending == null
    engine.getMappings(V1)._meta.elastic_protos.hash == DEFAULT.map(Document).contentHash()
  }

  def "a write after the swap wins even when the reconcile left the new index behind the old version numbers"() {
    given: 'd0 written twice after the catch-up, so the reconcile writes it into V2 one version behind V1'
    on[Phase.CAUGHT_UP] = { put('d0', 'a'); put('d0', 'b') }
    migrate()
    assert engine.get(V1, 'd0').get().version() > engine.get(V2, 'd0').get().version()

    and: 'one write after the swap brings V2 level with the frozen V1'
    put('d0', 'c')
    assert engine.get(V1, 'd0').get().version() == engine.get(V2, 'd0').get().version()

    when:
    def outcome = migrator().rollback(plan, V1, RollbackMode.REVERSE_DELTA)

    then: 'the rollback compares sources, so the write after the swap is carried back'
    outcome.ok()
    titles(V1).d0 == 'c'
  }

  def "writes during the rollback land, and its marker records the rollback"() {
    given:
    migrate()
    Map marker = null
    on[Phase.CAUGHT_UP] = { put('d2', 'during rollback'); marker = engine.getMappings(V1)._meta.elastic_protos.pending }
    on[Phase.SWAPPED] = { put('d3', 'after rollback swap') }

    when:
    def outcome = migrator().rollback(plan)

    then:
    outcome.ok()
    marker == [action: 'reindex', from: V2, phase: 'copying', rollback: true]
    titles(V1).d2 == 'during rollback'
    titles(V1).d3 == 'after rollback swap'
  }

  def "a rollback that fails before the swap leaves the alias on the new index"() {
    given:
    migrate()
    engine.duringReindex { throw new EngineException(EngineException.Kind.TRANSIENT, 503, 'busy') }

    when:
    def outcome = migrator().rollback(plan)

    then:
    outcome.status() == MigrationResult.Status.FAILED
    outcome.index() == V2
    outcome.error().message.contains('aborted before the alias swap')
    outcome.error().message.contains('RollbackMode.ALIAS_ONLY')
    engine.indices() == [(V1): [] as Set, (V2): ['documents'] as Set]
    engine.isWriteBlocked(V1)
    !engine.isWriteBlocked(V2)
    engine.getMappings(V1)._meta.elastic_protos.pending == null
  }

  def "a rollback whose alias swap fails is aborted the same way"() {
    given:
    engine = new SwapFailingEngine()
    setup()
    migrate()
    engine.failSwaps = true

    when:
    def outcome = migrator().rollback(plan)

    then:
    outcome.status() == MigrationResult.Status.FAILED
    outcome.error().message.contains('aborted before the alias swap: nope')
    engine.indices()[V2] == ['documents'] as Set
    engine.isWriteBlocked(V1)
    engine.openPits().isEmpty()
  }

  def "an ALIAS_ONLY rollback moves the alias and blocks the index it moved from"() {
    given:
    migrate()
    put('n1', 'not carried back')

    when:
    def outcome = migrator().rollback(plan, V1, RollbackMode.ALIAS_ONLY)

    then:
    outcome.status() == MigrationResult.Status.APPLIED
    outcome.index() == V1
    outcome.retainedIndices() == [V2]
    engine.indices() == [(V1): ['documents'] as Set, (V2): [] as Set]
    !engine.isWriteBlocked(V1)
    engine.isWriteBlocked(V2)
    !engine.documents(V1).containsKey('n1')
  }

  def "a rollback refuses when the alias is not on the new index only"() {
    given:
    migrate()
    migrator().rollback(plan, V1, RollbackMode.ALIAS_ONLY)

    when:
    def outcome = migrator().rollback(plan, V1, mode)

    then:
    outcome.status() == MigrationResult.Status.FAILED
    outcome.error().message == "alias documents points at [${V1}], not at ${V2} only; nothing was rolled back".toString()

    where:
    mode << RollbackMode.values()
  }

  def "a crash after the rollback swap is resumed by the next plan"() {
    given:
    migrate()
    put('n1', 'after the reindex')
    on[Phase.FROZEN] = { throw new IllegalStateException('killed') }
    def crashed = migrator().rollback(plan)
    on.clear()

    when: 'the next plan finds the rollback marker on the index the alias now points at'
    def resumed = migrator().plan(Document)
    def result = migrator().apply(resumed)

    then:
    crashed.status() == MigrationResult.Status.FAILED
    crashed.index() == V1
    resumed.items()[0].resume().rollback()
    resumed.items()[0].resume().from() == V2
    resumed.items()[0].reason().startsWith("resuming the interrupted rollback from ${V2} into ${V1}".toString())
    result.ok()
    result.outcomes()[0].retainedIndices() == [V2]
    titles(V1).n1 == 'after the reindex'
    engine.getMappings(V1)._meta.elastic_protos.pending == null
    engine.indexExists(V2)
  }

  def "the old void-returning overloads delegate: rollback(plan) uses the plan's current index"() {
    given:
    migrate(ZD.withReindexMode(ReindexMode.BLOCKING))

    when:
    def outcome = migrator().rollback(plan)

    then:
    outcome.ok()
    engine.indices()[V1] == ['documents'] as Set

    when:
    migrator().rollback(new IndexPlan(plan.target(), Action.CREATE, null, null, MappingDiff.empty(), 'x', []))

    then:
    thrown(MigrationException)
  }

  def "a crash during the rollback copy stops without cleanup"() {
    given:
    migrate()
    on[Phase.COPIED] = { throw new IllegalStateException('killed') }

    when:
    def outcome = migrator().rollback(plan)

    then:
    outcome.status() == MigrationResult.Status.FAILED
    outcome.index() == V2
    outcome.error().message == 'documents: stopped by the listener after COPIED: killed'
    engine.getMappings(V1)._meta.elastic_protos.pending.phase == 'copying'
  }

  /** Refuses alias swaps while {@code failSwaps} is set. */
  static class SwapFailingEngine extends InMemorySearchEngine {
    boolean failSwaps

    @Override
    void updateAliases(List<com.phatjam98.elasticprotos.engine.AliasAction> actions) {
      if (failSwaps) {
        throw new EngineException(EngineException.Kind.BAD_REQUEST, 400, 'nope')
      }
      super.updateAliases(actions)
    }
  }
}
