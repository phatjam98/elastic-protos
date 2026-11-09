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

/** Zero-downtime reindexing: copy, catch up, swap, freeze, reconcile, sweep, and resuming it. */
class IndexMigratorZeroDowntimeSpec extends Specification {

  static final ProtoMapper DEFAULT = ProtoMapper.withDefaults()
  static final ProtoMapper TITLE_KEYWORD = ProtoMapper.builder()
      .overrides(MappingOverrides.builder().forPath('title') { it.setType(EsType.KEYWORD) }.build())
      .build()
  static final String V1 = DEFAULT.map(Document).name()
  static final String V2 = TITLE_KEYWORD.map(Document).name()
  static final MigrationOptions ZD = MigrationOptions.standard().withSwapGrace(Duration.ZERO)

  InMemorySearchEngine engine = new InMemorySearchEngine()
  /** Actions run by the listener when a phase completes. */
  Map<Phase, Closure> on = [:]
  List<Phase> phases = []

  IndexMigrator migrator(Map args = [:]) {
    def builder = IndexMigrator.builder().engine(args.engine ?: engine)
        .mapper(args.mapper ?: TITLE_KEYWORD)
        .options(args.options ?: ZD)
        .scriptProvider(args.scripts ?: MigrationScriptProvider.none())
    if (args.listen != false) {
      builder.listener({ alias, phase ->
        phases << phase
        on[phase]?.call()
      } as MigrationListener)
    }
    builder.build()
  }

  def setup() {
    IndexMigrator.builder().engine(engine).mapper(DEFAULT).build().bootstrap(Document)
    5.times { put("d${it}", "Title ${it}") }
  }

  void put(String id, String title) {
    engine.index('documents', id, [id: id, title: title, body: 'b'], IndexOptions.defaults())
  }

  void remove(String id) {
    engine.delete('documents', id)
  }

  Map<String, String> titles(String index) {
    engine.documents(index).collectEntries { id, doc -> [(id): doc.title] }
  }

  Map pending(String index) {
    engine.getMappings(index)._meta.elastic_protos.pending
  }

  def "a reindex never blocks the live index and lands every write made in every window"() {
    given:
    on[Phase.COPIED] = { put('d0', 'copy'); remove('d1'); put('n1', 'new 1') }
    on[Phase.CAUGHT_UP] = { put('d0', 'catch-up 1'); put('d0', 'catch-up 2'); remove('d2'); put('n2', 'new 2'); put('d3', 'pre-swap') }
    on[Phase.SWAPPED] = { put('d0', 'after-swap'); remove('d4'); put('n3', 'new 3') }
    List<Boolean> blockedDuringCopy = []
    engine.duringReindex { blockedDuringCopy << engine.isWriteBlocked(V1) }

    when:
    def result = migrator().bootstrap(Document)
    def outcome = result.outcomes()[0]

    then: 'writes never hit a blocked index before the swap'
    result.ok()
    blockedDuringCopy == [false, false]

    and: 'the last write wins per document, and deletions on either side stick'
    titles(V2) == [d0: 'after-swap', d3: 'pre-swap', n1: 'new 1', n2: 'new 2', n3: 'new 3']
    engine.tombstoneVersion(V2, 'd4') != null

    and: 'the reconcile reports what it did'
    outcome.reconcile() == new ReconcileStats(2, 1, 2, 0)
    outcome.sourceCount() == 5L
    outcome.destCount() == 5L
    outcome.warnings().isEmpty()

    and: 'the alias moved, the old index is retained frozen, the marker is gone'
    engine.indices() == [(V1): [] as Set, (V2): ['documents'] as Set]
    outcome.index() == V2
    outcome.retainedIndices() == [V1]
    engine.isWriteBlocked(V1)
    !engine.isWriteBlocked(V2)
    pending(V2) == null
    engine.openPits().isEmpty()
    phases == [Phase.PREPARED, Phase.COPIED, Phase.CAUGHT_UP, Phase.SNAPSHOT, Phase.SWAPPED, Phase.FROZEN,
               Phase.RECONCILED, Phase.SWEPT, Phase.FINISHED]
    migrator().plan(Document).items()[0].action() == Action.NO_OP
  }

  def "the copy runs under external versioning from the old index and writes during it are caught up"() {
    given:
    engine.duringReindex { request ->
      if (request.dest() == V2 && engine.documents(V2).isEmpty()) {
        put('d0', 'during copy')
      }
    }

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.ok()
    titles(V2).d0 == 'during copy'
    result.outcomes()[0].reconcile() == new ReconcileStats(0, 0, 0, 0)
  }

  def "the operations follow the protocol"() {
    given:
    engine.clearLog()

    when:
    migrator().bootstrap(Document)
    def writes = engine.log().findAll {
      it ==~ /(createIndex|deleteIndex|putMappings|reindex|openPit|closePit|updateAliases|setWriteBlock|bulk|refresh) .*/
    }.collect { it.replaceAll(/[0-9a-f-]{36}/, 'PIT') }

    then:
    writes == [
        "createIndex ${V2}",
        "putMappings ${V2}",
        "refresh ${V1}",
        "reindex ${V1} -> ${V2}",
        "refresh ${V1}",
        "reindex ${V1} -> ${V2}",
        "refresh ${V2}",
        "openPit ${V2}",
        "putMappings ${V2}",
        "updateAliases [remove ${V1} documents, add ${V2} documents]",
        "setWriteBlock ${V1}=true",
        "refresh ${V1}",
        "putMappings ${V2}",
        "openPit ${V1}",
        "closePit PIT",
        "openPit ${V1}",
        "closePit PIT",
        "putMappings ${V2}",
        "closePit PIT",
        "putMappings ${V2}",
        "refresh ${V2}",
    ]*.toString()
  }

  def "the marker on the new index records each phase and is gone at the end"() {
    given:
    Map<Phase, Map> seen = [:]
    Phase.values().each { phase -> on[phase] = { seen[phase] = pending(V2) } }

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.ok()
    seen[Phase.PREPARED] == [action: 'reindex', from: V1, phase: 'copying', checkpoint: 4]
    seen[Phase.COPIED] == seen[Phase.PREPARED]
    seen[Phase.CAUGHT_UP] == seen[Phase.PREPARED]
    seen[Phase.SNAPSHOT].subMap('action', 'from', 'phase') == [action: 'reindex', from: V1, phase: 'swapping']
    seen[Phase.SNAPSHOT].pit instanceof String
    seen[Phase.SWAPPED] == seen[Phase.SNAPSHOT]
    seen[Phase.FROZEN] == seen[Phase.SNAPSHOT] + [phase: 'reconciling']
    seen[Phase.RECONCILED] == seen[Phase.FROZEN]
    seen[Phase.SWEPT] == seen[Phase.FROZEN]
    seen[Phase.FINISHED] == null
    engine.getMappings(V2)._meta.elastic_protos.keySet() == ['hash', 'proto', 'schema_version'] as Set
  }

  def "a migration script runs on reconciled documents through a staging index"() {
    given:
    MigrationScriptProvider scripts = { alias, from, to -> Optional.of('ctx._source.migrated = params.to_hash') }
    on[Phase.CAUGHT_UP] = { put('n1', 'new 1'); put('d0', 'pre-swap') }
    on[Phase.SWAPPED] = { put('n2', 'after swap') }
    String hash = TITLE_KEYWORD.map(Document).contentHash()

    when:
    def result = migrator(scripts: scripts).bootstrap(Document)
    def docs = engine.documents(V2)

    then:
    result.ok()
    docs.keySet() == ['d0', 'd1', 'd2', 'd3', 'd4', 'n1', 'n2'] as Set
    docs.findAll { id, doc -> id != 'n2' }.every { id, doc -> doc.migrated == hash }
    docs.n2.migrated == null
    docs.d0.title == 'pre-swap'
    result.outcomes()[0].reconcile() == new ReconcileStats(2, 0, 0, 0)
    result.outcomes()[0].warnings() == ["${V1} holds 6 documents and ${V2} 7; writes through the alias after the swap and the migration script account for this".toString()]
    !engine.indexExists(V2 + IndexMigrator.STAGING_SUFFIX)
    engine.log().contains("createIndex ${V2}-reconcile".toString())
  }

  def "a document the script drops while reconciling is dropped from the new index too"() {
    given: 'a staging copy that writes nothing, as if the script had set ctx.op'
    engine = new NoopStagingEngine()
    setup()
    MigrationScriptProvider scripts = { alias, from, to -> Optional.of('ctx._source.migrated = params.to_hash') }
    on[Phase.CAUGHT_UP] = { put('d0', 'pre-swap'); put('n1', 'new') }

    when:
    def result = migrator(scripts: scripts).bootstrap(Document)

    then:
    result.ok()
    !engine.documents(V2).containsKey('d0')
    !engine.documents(V2).containsKey('n1')
    result.outcomes()[0].reconcile() == new ReconcileStats(0, 0, 1, 0)
  }

  def "a script that changes document ids is caught by the probe before the swap"() {
    given:
    MigrationScriptProvider scripts = { alias, from, to -> Optional.of('ctx._id = params.alias') }

    when:
    def result = migrator(scripts: scripts).bootstrap(Document)
    def outcome = result.outcomes()[0]

    then:
    !result.ok()
    outcome.status() == MigrationResult.Status.FAILED
    outcome.index() == V1
    outcome.error().message.startsWith('the migration script changes _id; use ReindexMode.BLOCKING or fix the script')
    engine.indices() == [(V1): ['documents'] as Set]
    !engine.isWriteBlocked(V1)
    phases == [Phase.PREPARED, Phase.COPIED, Phase.CAUGHT_UP]
  }

  def "a script that changes only some ids gets past the probe and is caught by the reconcile"() {
    given: 'the probe sees only d0..d4; n1, written after the catch-up, gets an id the script changes'
    engine = new IdChangingStagingEngine()
    setup()
    MigrationScriptProvider scripts = { alias, from, to -> Optional.of('ctx._source.migrated = params.to_hash') }
    on[Phase.SNAPSHOT] = { put('n1', 'new'); (engine as IdChangingStagingEngine).changeIds = true }

    when:
    def result = migrator(scripts: scripts).bootstrap(Document)
    def outcome = result.outcomes()[0]

    then:
    outcome.status() == MigrationResult.Status.FAILED
    outcome.index() == V2
    outcome.error().message.contains('a script must not change _id')
    outcome.error().message.contains('resume the reconcile')
    pending(V2).phase == 'reconciling'
  }

  def "a reconcile write failing for another reason than a conflict fails the item"() {
    given:
    def failing = new FailingBulkEngine()
    engine = failing
    setup()
    on[Phase.CAUGHT_UP] = { put('n1', 'new') }

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
    result.outcomes()[0].error().message.contains('1 of 1 reconcile writes failed, first create')
    result.outcomes()[0].error().message.contains('507 disk full')
  }

  def "a failure before the swap deletes the new index and leaves the old one writable"() {
    given:
    engine.duringReindex { throw new EngineException(EngineException.Kind.TRANSIENT, 503, 'busy') }

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
    result.outcomes()[0].error() instanceof MigrationException
    result.outcomes()[0].error().message.contains('busy')
    engine.indices() == [(V1): ['documents'] as Set]
    !engine.isWriteBlocked(V1)
    !engine.log().any { it.startsWith('setWriteBlock') }
  }

  def "an incomplete copy pass fails before the swap"() {
    given:
    engine = new IncompleteReindexEngine()
    setup()

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
    result.outcomes()[0].error().message == "copy ${V1} -> ${V2} did not complete: only 4 of 5 documents accounted for".toString()
    engine.indices() == [(V1): ['documents'] as Set]
  }

  def "a stale write block on the live index is cleared before the copy"() {
    given:
    engine.setWriteBlock(V1, true)
    boolean blocked = true
    on[Phase.COPIED] = { blocked = engine.isWriteBlocked(V1) }

    when:
    def plan = migrator().plan(Document)
    def result = migrator().apply(plan)

    then:
    plan.items()[0].writeBlocked()
    plan.items()[0].warnings().any { it.contains('it will be cleared (clearStaleWriteBlock)') }
    result.ok()
    !blocked
  }

  def "count differences after the swap are warnings, and deleteOldAfter deletes the old index"() {
    given:
    on[Phase.SWAPPED] = { put('n1', 'after swap') }

    when:
    def result = migrator(options: ZD.withDeleteOldAfter(true)).bootstrap(Document)
    def outcome = result.outcomes()[0]

    then:
    result.ok()
    outcome.sourceCount() == 5L
    outcome.destCount() == 6L
    outcome.warnings() == ["${V1} holds 5 documents and ${V2} 6; writes through the alias after the swap account for this".toString()]
    outcome.retainedIndices().isEmpty()
    engine.indices() == [(V2): ['documents'] as Set]
  }

  def "a swap grace waits before the old index is frozen"() {
    given:
    long swapped = 0
    long frozen = 0
    on[Phase.SWAPPED] = { swapped = System.nanoTime() }
    on[Phase.FROZEN] = { frozen = System.nanoTime() }

    when:
    def result = migrator(options: ZD.withSwapGrace(Duration.ofMillis(50))).bootstrap(Document)

    then:
    result.ok()
    frozen - swapped >= 50_000_000L
  }

  def "an alias swap that reports a failure but took effect continues with the reconcile"() {
    given:
    engine = new FlakySwapEngine()
    setup()

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.ok()
    engine.indices()[V2] == ['documents'] as Set
  }

  def "a failed alias swap closes the snapshot and deletes the new index"() {
    given:
    engine = new FailingSwapEngine()
    setup()

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
    result.outcomes()[0].error().message.startsWith("alias swap ${V1} -> ${V2} failed".toString())
    engine.indices() == [(V1): ['documents'] as Set]
    engine.openPits().isEmpty()
  }

  // ------------------------------------------------------------------ crashes and resuming

  def "a crash after #phase is picked up by the next plan and apply"() {
    given: 'writes after the catch-up, or at the crash when it comes earlier'
    boolean wrote = false
    def writes = { if (!wrote) { wrote = true; put('d0', 'pre-swap'); remove('d1'); put('n1', 'new 1') } }
    on[Phase.CAUGHT_UP] = writes
    on[phase] = { writes(); throw new MigrationListener.Abort('killed') }

    when: 'the run dies after the phase'
    def crashed = migrator().bootstrap(Document)

    then:
    crashed.outcomes()[0].status() == MigrationResult.Status.FAILED
    crashed.outcomes()[0].error().message == "documents: stopped by the listener after ${phase}: killed".toString()
    crashed.outcomes()[0].index() == (swapped ? V2 : V1)

    when: 'writes go on through the alias, and a new process plans and applies'
    put('d2', 'after crash')
    put('n2', 'after crash')
    on.clear()
    def plan = migrator(listen: false).plan(Document)
    def result = migrator(listen: false).apply(plan)

    then:
    plan.items()[0].action() == action
    plan.items()[0].resume()?.swapped() == resumeSwapped
    result.ok()
    titles(V2) == [d0: 'pre-swap', d2: 'after crash', d3: 'Title 3', d4: 'Title 4', n1: 'new 1', n2: 'after crash']
    engine.indices()[V2] == ['documents'] as Set
    engine.isWriteBlocked(V1)
    pending(V2) == null
    migrator(listen: false).plan(Document).items()[0].action() == Action.NO_OP

    where:
    phase            | swapped | action         | resumeSwapped
    Phase.PREPARED   | false   | Action.REINDEX | false
    Phase.COPIED     | false   | Action.REINDEX | false
    Phase.CAUGHT_UP  | false   | Action.REINDEX | false
    Phase.SNAPSHOT   | false   | Action.REINDEX | false
    Phase.SWAPPED    | true    | Action.REINDEX | true
    Phase.FROZEN     | true    | Action.REINDEX | true
    Phase.RECONCILED | true    | Action.REINDEX | true
    Phase.SWEPT      | true    | Action.REINDEX | true
    Phase.FINISHED   | true    | Action.NO_OP   | null
  }

  def "a resumed reindex after the swap never copies with _reindex again"() {
    given:
    on[Phase.FROZEN] = { throw new MigrationListener.Abort('killed') }
    migrator().bootstrap(Document)
    engine.clearLog()

    when:
    def plan = migrator(listen: false).plan(Document)
    def result = migrator(listen: false).apply(plan)

    then:
    result.ok()
    plan.items()[0].currentIndex() == V1
    plan.items()[0].target().name() == V2
    plan.items()[0].resume().from() == V1
    plan.items()[0].resume().phase() == 'reconciling'
    plan.items()[0].resume().pitId() != null
    !plan.items()[0].resume().rollback()
    plan.items()[0].reason() == "resuming the interrupted reindex from ${V1} into ${V2}, which the alias already points at (phase reconciling)".toString()
    plan.toJson().contains('"phase" : "reconciling"')
    !engine.log().any { it.startsWith('reindex ') }
  }

  def "a crash before the swap restarts from the copy into the same index"() {
    given:
    on[Phase.SNAPSHOT] = { throw new MigrationListener.Abort('killed') }
    migrator().bootstrap(Document)

    when:
    def plan = migrator(listen: false).plan(Document)

    then:
    plan.items()[0].action() == Action.REINDEX
    plan.items()[0].resume() == new ReindexResume(V1, pending(V2).pit, 'swapping', false, false)
    plan.items()[0].reason().endsWith("an interrupted zero-downtime reindex into ${V2} (phase swapping) restarts from the copy".toString())

    when: 'a blocking run picks it up instead'
    def blocking = IndexMigrator.builder().engine(engine).mapper(TITLE_KEYWORD)
        .options(ZD.withReindexMode(ReindexMode.BLOCKING)).build()
    def blockingPlan = blocking.plan(Document)
    def result = blocking.apply(blockingPlan)

    then: 'it recreates the index, since a marked leftover is not a blocking copy'
    blockingPlan.items()[0].resume() == null
    result.ok()
    pending(V2) == null
    engine.documents(V2).size() == 5
    engine.log().contains("deleteIndex ${V2}".toString())
  }

  def "a resume before the swap closes the point in time of the interrupted run"() {
    given:
    on[Phase.SWAPPED] = { }
    def failing = new FailingSwapEngine()
    engine = failing
    setup()
    migrator().bootstrap(Document)
    assert engine.openPits().isEmpty()

    when: 'a run aborted at SNAPSHOT closes its point in time too'
    engine = new InMemorySearchEngine()
    setup()
    on[Phase.SNAPSHOT] = { throw new MigrationListener.Abort('killed') }
    migrator().bootstrap(Document)

    then:
    engine.openPits().isEmpty()
    pending(V2).pit != null

    when: 'the resume closes the recorded point in time before it opens a new one'
    engine.clearLog()
    on.clear()
    migrator(listen: false).bootstrap(Document)

    then:
    engine.log().findIndexOf { it.startsWith('closePit') } < engine.log().findIndexOf { it.startsWith('openPit') }
  }

  def "only copying and swapping markers restart a copy"() {
    given:
    on[Phase.SNAPSHOT] = { throw new MigrationListener.Abort('killed') }
    migrator().bootstrap(Document)
    def marker = pending(V2) + [phase: 'reconciling']
    engine.putMappings(V2, [_meta: [elastic_protos: engine.getMappings(V2)._meta.elastic_protos + [pending: marker]]])

    expect:
    migrator(listen: false).plan(Document).items()[0].resume() == null
  }

  def "a listener that fails with anything but Abort is ignored"() {
    given:
    on[Phase.COPIED] = { throw new IllegalStateException('boom') }

    expect:
    migrator().bootstrap(Document).ok()
  }

  def "a version reset after gc_deletes is caught by the sequence number checkpoint"() {
    given: 'd0 is deleted, its tombstone expires, and it is written again at version 1, after the catch-up'
    on[Phase.CAUGHT_UP] = {
      remove('d0')
      engine.expireTombstones()
      put('d0', 'reborn')
      assert engine.get(V1, 'd0').get().version() == 1L
    }

    when:
    def result = migrator(engine: engineWith(checkpoint)).bootstrap(Document)

    then:
    result.ok()
    titles(V2).d0 == expected

    where:
    checkpoint | expected
    'real'     | 'reborn'
    'useless'  | 'Title 0'
  }

  def "with a gc_deletes lifetime, tombstones expire on their own"() {
    given:
    engine.gcDeletes(Duration.ZERO)
    remove('d0')

    expect:
    engine.tombstoneVersion(V1, 'd0') == null
    engine.gcDeletes(null).is(engine)
  }

  def "an engine without checkpoints treats every document as possibly changed"() {
    given:
    engine = new NoCheckpointEngine()
    setup()
    on[Phase.CAUGHT_UP] = { put('d0', 'pre-swap') }

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.ok()
    titles(V2).d0 == 'pre-swap'
    pending(V2) == null
  }

  def "a lost snapshot is reported, not guessed at: INCOMPLETE, nothing written, marker kept"() {
    given:
    on[Phase.CAUGHT_UP] = { put('d0', 'pre-swap'); remove('d1'); put('n1', 'new 1') }
    on[Phase.SWAPPED] = { remove('d3') }
    on[Phase.FROZEN] = { throw new MigrationListener.Abort('killed') }
    migrator().bootstrap(Document)
    engine.expirePits()
    engine.clearLog()

    when:
    def result = migrator(listen: false).bootstrap(Document)
    def outcome = result.outcomes()[0]

    then: 'd3, deleted through the alias after the swap, is not brought back'
    !result.ok()
    outcome.status() == MigrationResult.Status.INCOMPLETE
    outcome.index() == V2
    outcome.retainedIndices() == [V1]
    outcome.warnings()[0] == "the snapshot of ${V2} taken at the swap was lost; nothing was written. 2 documents of ${V1}".toString() +
        " are missing from ${V2}, 1 differ, and 1 exist only in ${V2}. Missing documents may have been written before the swap".toString() +
        ' or deleted through the alias after it; apply again with DegradedReconcile.CREATE_MISSING to create them, or remove the pending marker'
    !engine.log().any { it.startsWith('bulk') }
    !engine.documents(V2).containsKey('d3')
    pending(V2).phase == 'reconciling'
    migrator(listen: false).plan(Document).items()[0].resume().swapped()
  }

  def "with CREATE_MISSING a lost snapshot creates missing documents and reports what it could not decide"() {
    given:
    on[Phase.CAUGHT_UP] = { put('d0', 'pre-swap'); remove('d1'); put('n1', 'new 1') }
    on[Phase.FROZEN] = { throw new MigrationListener.Abort('killed') }
    migrator().bootstrap(Document)
    engine.expirePits()

    when:
    def result = migrator(listen: false, options: ZD.withDegradedReconcile(DegradedReconcile.CREATE_MISSING)).bootstrap(Document)
    def outcome = result.outcomes()[0]

    then:
    result.ok()
    outcome.status() == MigrationResult.Status.APPLIED
    outcome.reconcile() == new ReconcileStats(1, 0, 0, 1)
    outcome.warnings()[0] == "the snapshot of ${V2} taken at the swap was lost, so missing documents were created (including any deleted".toString() +
        " through the alias since the swap): 1 created, 1 documents differ between ${V1} and ${V2} and were left as they are in ${V2}, and 1 documents exist only in ${V2} and were kept".toString()
    titles(V2).n1 == 'new 1'
    titles(V2).d0 == 'Title 0'
    pending(V2) == null
  }

  def "a lost snapshot with nothing to decide still reports, and INCOMPLETE stays until the operator decides"() {
    given:
    on[Phase.FROZEN] = { throw new MigrationListener.Abort('killed') }
    migrator().bootstrap(Document)
    engine.expirePits()

    when:
    def result = migrator(listen: false).bootstrap(Document)
    def again = migrator(listen: false).bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.INCOMPLETE
    result.outcomes()[0].warnings()[0].contains('0 documents of')
    again.outcomes()[0].status() == MigrationResult.Status.INCOMPLETE
  }

  def "a resume whose old index is gone only removes the marker"() {
    given:
    on[Phase.FROZEN] = { throw new MigrationListener.Abort('killed') }
    migrator().bootstrap(Document)
    engine.deleteIndex(V1)

    when:
    def plan = migrator(listen: false).plan(Document)
    def result = migrator(listen: false).apply(plan)

    then:
    plan.items()[0].currentHash() == null
    result.ok()
    result.outcomes()[0].warnings() == ["${V1} no longer exists, so nothing was reconciled into ${V2}".toString()]
    pending(V2) == null
  }

  def "a resume after the proto changed again warns and finishes the interrupted reindex first"() {
    given:
    on[Phase.FROZEN] = { throw new MigrationListener.Abort('killed') }
    migrator().bootstrap(Document)

    when:
    def plan = migrator(listen: false, mapper: DEFAULT).plan(Document)

    then:
    plan.items()[0].action() == Action.REINDEX
    plan.items()[0].target().name() == V2
    plan.items()[0].warnings()[0].contains('plan again once the interrupted reindex has finished')
  }

  def "a failure while reconciling keeps the marker so the next run resumes"() {
    given:
    def failing = new FailingBulkEngine()
    engine = failing
    setup()
    on[Phase.CAUGHT_UP] = { put('n1', 'new') }
    def first = migrator().bootstrap(Document)
    failing.fail = false

    when:
    def result = migrator(listen: false).bootstrap(Document)

    then:
    first.outcomes()[0].status() == MigrationResult.Status.FAILED
    first.outcomes()[0].retainedIndices() == [V1]
    result.ok()
    engine.documents(V2).containsKey('n1')
  }

  def "a dry run of a resume reports the target index"() {
    given:
    on[Phase.FROZEN] = { throw new MigrationListener.Abort('killed') }
    migrator().bootstrap(Document)

    when:
    def result = migrator(listen: false, options: ZD.withDryRun(true)).bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.DRY_RUN
    result.outcomes()[0].index() == V2
  }

  // ------------------------------------------------------------------ blocking parity

  def "BLOCKING keeps the blocking procedure and its operations"() {
    given:
    def plan = migrator(options: ZD.withReindexMode(ReindexMode.BLOCKING)).plan(Document)
    engine.clearLog()

    when:
    def result = migrator(options: ZD.withReindexMode(ReindexMode.BLOCKING)).apply(plan)

    then:
    result.ok()
    result.outcomes()[0].reconcile() == null
    phases == [Phase.COPIED, Phase.SWAPPED]
    engine.log() == [
        "indexExists ${V2}",
        'health',
        "createIndex ${V2}",
        "setWriteBlock ${V1}=true",
        "refresh ${V1}",
        "reindex ${V1} -> ${V2}",
        "search ${V1}",
        "search ${V2}",
        "updateAliases [remove ${V1} documents, add ${V2} documents]",
    ]*.toString()
  }

  def "a stale write block is described per mode when a reindex is planned"() {
    given:
    engine.setWriteBlock(V1, true)

    expect:
    migrator(options: options).plan(Document).items()[0].warnings().any { it.contains(text) }

    where:
    options                                         | text
    ZD                                              | 'it will be cleared (clearStaleWriteBlock)'
    ZD.withClearStaleWriteBlock(false)              | 'it is left in place (clearStaleWriteBlock is false)'
    ZD.withReindexMode(ReindexMode.BLOCKING)        | 'the reindex moves the alias to an unblocked index'
  }

  // ------------------------------------------------------------------ engines with faults

  /** Fails bulk creates with 507 while {@code fail} is set. */
  static class FailingBulkEngine extends InMemorySearchEngine {
    boolean fail = true

    @Override
    com.phatjam98.elasticprotos.engine.BulkResult bulk(List<com.phatjam98.elasticprotos.engine.BulkOp> operations) {
      if (fail && operations.any { it.type() == com.phatjam98.elasticprotos.engine.BulkOp.Type.CREATE }) {
        return new com.phatjam98.elasticprotos.engine.BulkResult(true, operations.collect {
          new com.phatjam98.elasticprotos.engine.BulkResult.Item('create', it.index(), it.id(), 507, 'disk full')
        }, 0)
      }
      super.bulk(operations)
    }
  }

  /** Switches to the engine variant a feature needs. */
  InMemorySearchEngine engineWith(String checkpoint) {
    if (checkpoint == 'useless') {
      def useless = new CheckpointEngine(fixed: Long.MAX_VALUE)
      copyDocuments(useless)
      engine = useless
    }
    engine
  }

  void copyDocuments(InMemorySearchEngine target) {
    // rebuild the starting state in the other engine
    IndexMigrator.builder().engine(target).mapper(DEFAULT).options(ZD)
        .build().bootstrap(Document)
    5.times { target.index('documents', "d${it}", [id: "d${it}".toString(), title: "Title ${it}".toString(), body: 'b'], IndexOptions.defaults()) }
  }

  /** Reports a fixed checkpoint, standing in for a reconcile without the checkpoint. */
  static class CheckpointEngine extends InMemorySearchEngine {
    long fixed

    @Override
    long seqNoCheckpoint(String index) {
      fixed
    }
  }

  /** Reports no checkpoint, like an engine that does not implement it. */
  static class NoCheckpointEngine extends InMemorySearchEngine {
    @Override
    long seqNoCheckpoint(String index) {
      -1
    }
  }

  /** Stages documents under another id once {@code changeIds} is set. */
  static class IdChangingStagingEngine extends InMemorySearchEngine {
    boolean changeIds

    @Override
    com.phatjam98.elasticprotos.engine.ReindexResult reindex(com.phatjam98.elasticprotos.engine.ReindexRequest request) {
      if (changeIds && request.dest().endsWith(IndexMigrator.STAGING_SUFFIX)) {
        return super.reindex(request.withScript('ctx._id = params.alias', request.scriptParams()))
      }
      super.reindex(request)
    }
  }

  /** Stages nothing into the reconcile index, as a script that drops every document would. */
  static class NoopStagingEngine extends InMemorySearchEngine {
    @Override
    com.phatjam98.elasticprotos.engine.ReindexResult reindex(com.phatjam98.elasticprotos.engine.ReindexRequest request) {
      if (request.dest().endsWith(IndexMigrator.STAGING_SUFFIX)) {
        long n = request.query().ids.values.size()
        return new com.phatjam98.elasticprotos.engine.ReindexResult(n, 0, 0, [], 0, false, n, 0, 0)
      }
      super.reindex(request)
    }
  }

  /** Reports one document fewer than it copied. */
  static class IncompleteReindexEngine extends InMemorySearchEngine {
    @Override
    com.phatjam98.elasticprotos.engine.ReindexResult reindex(com.phatjam98.elasticprotos.engine.ReindexRequest request) {
      def result = super.reindex(request)
      new com.phatjam98.elasticprotos.engine.ReindexResult(result.total(), result.created() - 1, result.updated(),
          [], 0, false, 0, 0, result.versionConflicts())
    }
  }

  /** Applies the alias swap, then reports a failure. */
  static class FlakySwapEngine extends InMemorySearchEngine {
    @Override
    void updateAliases(List<com.phatjam98.elasticprotos.engine.AliasAction> actions) {
      super.updateAliases(actions)
      if (actions.size() == 2) {
        throw new EngineException(EngineException.Kind.IO, 0, 'connection reset')
      }
    }
  }

  /** Refuses the alias swap. */
  static class FailingSwapEngine extends InMemorySearchEngine {
    @Override
    void updateAliases(List<com.phatjam98.elasticprotos.engine.AliasAction> actions) {
      if (actions.size() == 2) {
        throw new EngineException(EngineException.Kind.BAD_REQUEST, 400, 'nope')
      }
      super.updateAliases(actions)
    }
  }
}
