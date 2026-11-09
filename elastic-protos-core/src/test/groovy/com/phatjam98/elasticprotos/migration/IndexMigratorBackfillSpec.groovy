package com.phatjam98.elasticprotos.migration

import com.phatjam98.elasticprotos.engine.IndexOptions
import com.phatjam98.elasticprotos.engine.ReindexResult
import com.phatjam98.elasticprotos.engine.UpdateByQueryRequest
import com.phatjam98.elasticprotos.mapping.MappingOverrides
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.migration.MigrationListener.Phase
import com.phatjam98.elasticprotos.proto.Bool3
import com.phatjam98.elasticprotos.proto.DynamicMode
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.testkit.InMemorySearchEngine
import java.time.Duration
import spock.lang.Specification

/** Additions existing documents were never indexed into are backfilled in place. */
class IndexMigratorBackfillSpec extends Specification {

  static final ProtoMapper DEFAULT = ProtoMapper.withDefaults()
  static final ProtoMapper BODY_KEYWORD = ProtoMapper.builder()
      .overrides(MappingOverrides.builder().forPath('body') { it.setKeywordSubfield(Bool3.TRUE) }.build())
      .build()
  static final String V1 = DEFAULT.map(Document).name()
  static final String HASH2 = BODY_KEYWORD.map(Document).contentHash()
  static final MigrationOptions OPTIONS = MigrationOptions.standard().withSwapGrace(Duration.ZERO).withLock(false)

  InMemorySearchEngine engine = new InMemorySearchEngine()
  List<Phase> phases = []
  Map<Phase, Closure> on = [:]

  IndexMigrator migrator(Map args = [:]) {
    IndexMigrator.builder().engine(engine)
        .mapper(args.mapper ?: BODY_KEYWORD)
        .options(args.options ?: OPTIONS)
        .scriptProvider(args.scripts ?: MigrationScriptProvider.none())
        .listener({ alias, phase -> phases << phase; on[phase]?.call() } as MigrationListener)
        .build()
  }

  def setup() {
    IndexMigrator.builder().engine(engine).mapper(DEFAULT).options(MigrationOptions.standard().withLock(false)).build().bootstrap(Document)
    3.times { put("d${it}", "body ${it}") }
  }

  void put(String id, String body) {
    engine.index('documents', id, [id: id, body: body], IndexOptions.defaults())
  }

  def "an added multi-field is backfilled in place: mapping, update by query, marker removed"() {
    given:
    engine.clearLog()

    when:
    def plan = migrator().plan(Document)

    then:
    plan.items()[0].action() == Action.BACKFILL
    plan.items()[0].currentIndex() == V1
    plan.items()[0].reason() == 'added [body.keyword], which existing documents were never indexed into:' +
        ' the mapping is extended in place and the documents re-indexed'
    plan.hasChanges()
    plan.toReport().contains("BACKFILL    documents: ${V1} (in place)")

    when:
    engine.clearLog()
    def result = migrator().apply(plan)

    then:
    result.ok()
    result.outcomes()[0].index() == V1
    engine.indices() == [(V1): ['documents'] as Set]
    engine.log() == ["putMappings ${V1}", "updateByQuery ${V1}", "putMappings ${V1}"]*.toString()
    engine.getMappings(V1).properties.body.fields.keyword.type == 'keyword'
    engine.getMappings(V1)._meta.elastic_protos == [hash: HASH2, proto: 'phatjam98.elasticprotos.example.Document',
                                                    schema_version: engine.getMappings(V1)._meta.elastic_protos.schema_version]
    engine.get(V1, 'd0').get().version() == 2L
    phases == [Phase.BACKFILL_MARKED, Phase.BACKFILLED, Phase.BACKFILL_FINISHED]
    migrator().plan(Document).items()[0].action() == Action.NO_OP
  }

  def "the marker carries the target hash while the update by query runs"() {
    given:
    Map marker = null
    engine.duringUpdateByQuery { marker = engine.getMappings(V1)._meta.elastic_protos.pending }

    when:
    migrator().bootstrap(Document)

    then:
    marker == [action: 'backfill', to_hash: HASH2]
    engine.getMappings(V1)._meta.elastic_protos.pending == null
  }

  def "#label forces a reindex"() {
    when:
    def item = migrator(options: options, scripts: scripts).plan(Document).items()[0]

    then:
    item.action() == Action.REINDEX
    item.reason().endsWith(suffix)

    where:
    label                | options                         | scripts                                                                         | suffix
    'backfill off'       | OPTIONS.withBackfill(false)     | MigrationScriptProvider.none()                                                  | '(backfill is disabled)'
    'a migration script' | OPTIONS                         | ({ a, f, t -> Optional.of('ctx._source.x = params.alias') } as MigrationScriptProvider) | 'so the documents are copied through it)'
  }

  def "with reindexing forbidden, a backfill still applies"() {
    expect:
    migrator(options: OPTIONS.withAllowReindex(false)).bootstrap(Document).ok()
  }

  def "a backfill that does not complete fails and leaves the marker, so the next plan backfills again: #label"() {
    given:
    engine = new CannedUpdateEngine(answer: answer)
    setup()

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
    result.outcomes()[0].error().message == "backfill of ${V1} did not complete: ${message}; ${V1} keeps its backfill marker, so the next plan backfills it again".toString()
    engine.getMappings(V1)._meta.elastic_protos.pending.action == 'backfill'

    when:
    engine.answer = null
    def plan = migrator().plan(Document)

    then:
    plan.items()[0].action() == Action.BACKFILL
    plan.items()[0].reason() == "resuming the interrupted backfill of ${V1}".toString()
    migrator().apply(plan).ok()
    engine.getMappings(V1)._meta.elastic_protos.pending == null

    where:
    label       | answer                                                           | message
    'failures'  | new ReindexResult(3, 0, 2, [[cause: 'bad']], 1, false)            | '1 failures, first {cause=bad}'
    'timeout'   | new ReindexResult(3, 0, 3, [], 1, true)                           | 'timed out'
    'unaccounted' | new ReindexResult(3, 0, 2, [], 1, false)                        | 'only 2 of 3 documents accounted for'
  }

  def "a document written during the backfill is a counted conflict, indexed by its own write"() {
    given:
    engine.duringUpdateByQuery { put('d1', 'written during the backfill') }

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.ok()
    engine.documents(V1).d1.body == 'written during the backfill'
  }

  def "a crash during a backfill resumes on the next run"() {
    given:
    on[Phase.BACKFILL_MARKED] = { throw new MigrationListener.Abort('killed') }
    def crashed = migrator().bootstrap(Document)
    on.clear()

    when:
    def plan = migrator().plan(Document)
    def result = migrator().apply(plan)

    then:
    crashed.outcomes()[0].status() == MigrationResult.Status.FAILED
    crashed.outcomes()[0].index() == V1
    plan.items()[0].action() == Action.BACKFILL
    result.ok()
    engine.getMappings(V1)._meta.elastic_protos.pending == null
    migrator().plan(Document).items()[0].action() == Action.NO_OP
  }

  def "a stale write block is cleared before a backfill"() {
    given:
    engine.setWriteBlock(V1, true)

    when:
    def plan = migrator().plan(Document)
    def result = migrator().apply(plan)

    then:
    plan.items()[0].warnings().any { it.contains('it will be cleared (clearStaleWriteBlock)') }
    result.ok()
    !engine.isWriteBlocked(V1)
  }

  def "a dry run of a backfill reports the current index"() {
    when:
    def result = migrator(options: OPTIONS.withDryRun(true)).bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.DRY_RUN
    result.outcomes()[0].index() == V1
    engine.getMappings(V1).properties.body.fields == null
  }

  def "turning dynamic from false to true stays in place with a warning"() {
    given:
    def closed = ProtoMapper.builder().overrides(MappingOverrides.builder()
        .forMessage(Document.descriptor) { it.setDynamic(DynamicMode.DYNAMIC_FALSE) }.build()).build()
    def open = ProtoMapper.builder().overrides(MappingOverrides.builder()
        .forMessage(Document.descriptor) { it.setDynamic(DynamicMode.DYNAMIC_TRUE) }.build()).build()
    engine = new InMemorySearchEngine()
    IndexMigrator.builder().engine(engine).mapper(closed).options(OPTIONS).build().bootstrap(Document)

    when:
    def item = migrator(mapper: open).plan(Document).items()[0]

    then:
    item.action() == Action.PUT_MAPPING
    item.warnings() == ['<root> becomes dynamic: values existing documents carry under it that were not mapped stay' +
                            ' unindexed until those documents are written again']
  }

  /** Answers update by query with a canned result while {@code answer} is set. */
  static class CannedUpdateEngine extends InMemorySearchEngine {
    ReindexResult answer

    @Override
    ReindexResult updateByQuery(UpdateByQueryRequest request) {
      def real = super.updateByQuery(request)
      answer ?: real
    }
  }
}
