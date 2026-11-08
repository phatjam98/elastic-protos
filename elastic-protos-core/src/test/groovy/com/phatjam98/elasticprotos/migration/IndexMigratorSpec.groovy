package com.phatjam98.elasticprotos.migration

import com.phatjam98.elasticprotos.engine.AliasAction
import com.phatjam98.elasticprotos.engine.EngineException
import com.phatjam98.elasticprotos.engine.IndexOptions
import com.phatjam98.elasticprotos.engine.ReindexRequest
import com.phatjam98.elasticprotos.engine.ReindexResult
import com.phatjam98.elasticprotos.engine.SearchEngine
import com.phatjam98.elasticprotos.mapping.IndexNamingStrategy
import com.phatjam98.elasticprotos.mapping.MappingOverrides
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.DynamicMode
import com.phatjam98.elasticprotos.proto.EsType
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.proto.test.Skipped
import com.phatjam98.elasticprotos.testkit.InMemorySearchEngine
import com.phatjam98.elasticprotos.util.Json
import spock.lang.Specification

class IndexMigratorSpec extends Specification {

  static final ProtoMapper DEFAULT = ProtoMapper.withDefaults()

  /** Document.title becomes a keyword: a breaking type change. */
  static final ProtoMapper TITLE_KEYWORD = ProtoMapper.builder()
      .overrides(MappingOverrides.builder().forPath('title') { it.setType(EsType.KEYWORD) }.build())
      .build()

  /** Document without relevance: the default mapper then adds it. */
  static final ProtoMapper NO_RELEVANCE = ProtoMapper.builder()
      .overrides(MappingOverrides.builder().ignorePath('relevance').build())
      .build()

  /** Firm.labels opted in to an object with dynamic: true instead of the default flattened. */
  static final ProtoMapper OBJECT_LABELS = ProtoMapper.builder()
      .overrides(MappingOverrides.builder().forField(Firm.descriptor, 'labels') { it.setType(EsType.OBJECT) }.build())
      .build()

  static final String DOC_V1 = DEFAULT.map(Document).name()
  static final String DOC_V2 = TITLE_KEYWORD.map(Document).name()

  InMemorySearchEngine engine = new InMemorySearchEngine()

  IndexMigrator migrator(ProtoMapper mapper = DEFAULT, MigrationOptions options = MigrationOptions.standard(),
                         SearchEngine target = engine) {
    IndexMigrator.builder().engine(target).mapper(mapper).options(options).build()
  }

  void seedDocuments(int count = 3) {
    count.times { i ->
      engine.index('documents', "d${i}", [id: "d${i}".toString(), title: "Title ${i}".toString(), body: 'b'],
          IndexOptions.defaults())
    }
  }

  def "first bootstrap creates Firm and Document with shard and replica defaults"() {
    when:
    def result = migrator().bootstrap(Firm, Document)

    then:
    result.ok()
    result.outcomes()*.status == [MigrationResult.Status.APPLIED] * 2
    result.outcomes()*.plan*.action == [Action.CREATE, Action.CREATE]
    result.outcomes()*.index == [DEFAULT.map(Firm).name(), DOC_V1]
    engine.indices() == [(DEFAULT.map(Firm).name()): ['firms'] as Set, (DOC_V1): ['documents'] as Set]
    engine.getSettings('documents').index.number_of_shards == '1'
    engine.getSettings('documents').index.number_of_replicas == '0'
    engine.getSettings('firms').index.number_of_shards == '1'
    engine.getMappings('documents')._meta.elastic_protos.hash == DEFAULT.map(Document).contentHash()
  }

  def "replica policy and default shards drive new index settings"() {
    given:
    engine.dataNodes(nodes)
    def options = MigrationOptions.standard().withReplicas(policy).withDefaultShards(3)

    when:
    migrator(DEFAULT, options).bootstrap(Document, Firm)

    then:
    engine.getSettings('documents').index.number_of_replicas == replicas
    engine.getSettings('documents').index.number_of_shards == '3'
    engine.getSettings('firms').index.number_of_shards == '1'
    engine.log().count { it == 'health' } == healthCalls

    where:
    nodes | policy                               | replicas | healthCalls
    1     | ReplicaPolicy.dataNodesMinusOne(1)   | '0'      | 2
    3     | ReplicaPolicy.dataNodesMinusOne(1)   | '1'      | 2
    5     | ReplicaPolicy.dataNodesMinusOne(3)   | '3'      | 2
    5     | ReplicaPolicy.fixed(2)               | '2'      | 0
  }

  def "a second bootstrap is a no-op that touches nothing"() {
    given:
    migrator().bootstrap(Firm, Document)
    engine.clearLog()

    when:
    def result = migrator().bootstrap(Firm, Document)

    then:
    result.ok()
    result.outcomes()*.plan*.action == [Action.NO_OP, Action.NO_OP]
    result.outcomes()*.status == [MigrationResult.Status.SKIPPED] * 2
    result.outcomes()*.plan*.currentHash == [DEFAULT.map(Firm).contentHash(), DEFAULT.map(Document).contentHash()]
    engine.log().every { it.startsWith('get') }
    result.outcomes()*.plan*.reason == ["up to date (hash ${DEFAULT.map(Firm).contentHash()})".toString(),
                                        "up to date (hash ${DEFAULT.map(Document).contentHash()})".toString()]
    !migrator().plan(Firm, Document).hasChanges()
  }

  def "keys picked up by a dynamic map field are not reported as removed fields"() {
    given: 'a firm with labels.tier was indexed, so the live mapping gained labels.properties.tier'
    migrator(OBJECT_LABELS).bootstrap(Firm)
    String index = OBJECT_LABELS.map(Firm).name()
    engine.putMappings(index, [properties: [labels: [type: 'object', dynamic: true, properties: [
        tier: [type: 'text', fields: [keyword: [type: 'keyword', ignore_above: 256]]]]]]])
    engine.index('firms', 'f1', [id: 'f1', labels: [tier: 'gold']], IndexOptions.defaults())

    expect: 'the live mapping really carries the picked-up key'
    engine.getMappings('firms').properties.labels.properties.tier.type == 'text'

    when:
    def item = migrator(OBJECT_LABELS, MigrationOptions.standard().withRemovedFields(policy))
        .plan(Firm).items()[0]

    then:
    item.action() == Action.NO_OP
    item.warnings().isEmpty()
    !item.diff().hasFieldChanges()
    item.reason() == "up to date (hash ${OBJECT_LABELS.map(Firm).contentHash()})".toString()
    item.currentIndex() == index

    where:
    policy << [RemovedFieldPolicy.KEEP, RemovedFieldPolicy.REINDEX]
  }

  def "a map field is flattened by default, so its keys never reach the mapping"() {
    given:
    migrator().bootstrap(Firm)
    engine.index('firms', 'f1', [id: 'f1', labels: [tier: 'gold']], IndexOptions.defaults())

    when:
    def item = migrator(DEFAULT, MigrationOptions.standard().withRemovedFields(RemovedFieldPolicy.REINDEX))
        .plan(Firm).items()[0]

    then:
    engine.getMappings('firms').properties.labels == [type: 'flattened']
    item.action() == Action.NO_OP
    !item.diff().hasFieldChanges()
  }

  def "moving a map field from a dynamic object to flattened is a type change that reindexes"() {
    given:
    migrator(OBJECT_LABELS).bootstrap(Firm)

    when:
    def item = migrator().plan(Firm).items()[0]

    then:
    item.action() == Action.REINDEX
    item.diff().typeChanged()*.path() == ['labels']
  }

  def "messages marked skip are left out of the plan"() {
    when:
    def plan = migrator().plan([Skipped.descriptor, Document.descriptor])

    then:
    plan.items()*.alias() == ['documents']
  }

  def "an added field is applied in place and moves the recorded hash forward"() {
    given:
    migrator(NO_RELEVANCE).bootstrap(Document)
    String original = NO_RELEVANCE.map(Document).name()

    when:
    def plan = migrator().plan(Document)

    then:
    plan.items()[0].action() == Action.PUT_MAPPING
    plan.items()[0].diff().additiveOnly()
    plan.items()[0].diff().added()*.path == ['relevance']
    plan.items()[0].currentIndex() == original

    when:
    def result = migrator().apply(plan)

    then:
    result.ok()
    result.outcomes()[0].index() == original
    engine.indices() == [(original): ['documents'] as Set]
    engine.getMappings('documents')._meta.elastic_protos.hash == DEFAULT.map(Document).contentHash()
    engine.getMappings('documents').properties.relevance == [type: 'double']

    when: 'planning again: the index keeps its old name but its mappings match'
    def again = migrator().plan(Document)

    then:
    again.items()[0].action() == Action.NO_OP
    again.items()[0].currentHash() == DEFAULT.map(Document).contentHash()
    again.items()[0].currentIndex() == original
    again.items()[0].reason() == "up to date (hash ${DEFAULT.map(Document).contentHash()})".toString()
    again.toReport().contains("NO_OP       documents: ${original} (in place) (added 0, removed 0, changed 0)")
    again.toReport().contains("up to date (hash ${DEFAULT.map(Document).contentHash()})")
  }

  def "a new multi-field and a dynamic change are compatible"() {
    given:
    migrator().bootstrap(Document)
    def mapper = ProtoMapper.builder().overrides(MappingOverrides.builder()
        .forPath('body') { it.setKeywordSubfield(com.phatjam98.elasticprotos.proto.Bool3.TRUE) }
        .forMessage(Document.descriptor) { it.setDynamic(DynamicMode.DYNAMIC_FALSE) }
        .build()).build()

    when:
    def result = migrator(mapper).bootstrap(Document)

    then:
    result.ok()
    result.outcomes()[0].plan().action() == Action.PUT_MAPPING
    result.outcomes()[0].plan().diff().added()*.path == ['body.keyword']
    result.outcomes()[0].plan().diff().paramChanged()*.path == ['<root>:dynamic']
    engine.getMappings('documents').dynamic == 'false'
  }

  def "a breaking change reindexes, swaps the alias atomically and retains the old index"() {
    given:
    migrator().bootstrap(Document)
    seedDocuments()
    def plan = migrator(TITLE_KEYWORD).plan(Document)
    engine.clearLog()

    when:
    def result = migrator(TITLE_KEYWORD).apply(plan)
    List<String> log = new ArrayList<>(engine.log())

    then:
    plan.items()[0].action() == Action.REINDEX
    plan.items()[0].diff().typeChanged() == [new MappingDiff.Change('title', 'text', 'keyword')]
    result.ok()
    result.outcomes()[0].index() == DOC_V2
    result.outcomes()[0].retainedIndices() == [DOC_V1]
    engine.indices() == [(DOC_V1): [] as Set, (DOC_V2): ['documents'] as Set]
    engine.isWriteBlocked(DOC_V1)
    !engine.isWriteBlocked(DOC_V2)
    engine.documents(DOC_V2).keySet() == ['d0', 'd1', 'd2'] as Set
    engine.documents(DOC_V2)['d0'].body == 'b'
    engine.getMappings('documents').properties.title.type == 'keyword'
    log == [
        "indexExists ${DOC_V2}".toString(),
        'health',
        "createIndex ${DOC_V2}".toString(),
        "setWriteBlock ${DOC_V1}=true".toString(),
        "refresh ${DOC_V1}".toString(),
        "reindex ${DOC_V1} -> ${DOC_V2}".toString(),
        "search ${DOC_V1}".toString(),
        "search ${DOC_V2}".toString(),
        "updateAliases [remove ${DOC_V1} documents, add ${DOC_V2} documents]".toString(),
    ]

    when: 'rolling back'
    migrator(TITLE_KEYWORD).rollback(plan.items()[0], DOC_V1)

    then:
    engine.indices() == [(DOC_V1): ['documents'] as Set, (DOC_V2): [] as Set]
    !engine.isWriteBlocked(DOC_V1)
  }

  def "rollback without an explicit index uses the plan's current index"() {
    given:
    migrator().bootstrap(Document)
    def plan = migrator(TITLE_KEYWORD).plan(Document)
    migrator(TITLE_KEYWORD).apply(plan)

    when:
    migrator(TITLE_KEYWORD).rollback(plan.items()[0])

    then:
    engine.indices()[DOC_V1] == ['documents'] as Set

    when:
    migrator().rollback(migrator().plan(Firm).items()[0])

    then:
    thrown(MigrationException)
  }

  def "deleteOldAfter deletes the old index after the swap"() {
    given:
    migrator().bootstrap(Document)
    seedDocuments()

    when:
    def result = migrator(TITLE_KEYWORD, MigrationOptions.standard().withDeleteOldAfter(true))
        .bootstrap(Document)

    then:
    result.ok()
    result.outcomes()[0].retainedIndices().isEmpty()
    engine.indices() == [(DOC_V2): ['documents'] as Set]
    engine.log().last() == "deleteIndex ${DOC_V1}".toString()
  }

  def "a reindex into an index left over from a failed attempt reuses it"() {
    given:
    migrator().bootstrap(Document)
    seedDocuments(2)
    engine.createIndex(DOC_V2, [:], TITLE_KEYWORD.map(Document).mappings(), [] as Set)

    when:
    def result = migrator(TITLE_KEYWORD).bootstrap(Document)

    then: 'it records the target hash, so it is reused'
    result.ok()
    engine.log().count { it.startsWith("createIndex ${DOC_V2}") } == 1
    !engine.log().contains("deleteIndex ${DOC_V2}".toString())
    engine.documents(DOC_V2).size() == 2
  }

  def "a leftover target index with another hash or none is deleted and recreated: #label"() {
    given:
    migrator().bootstrap(Document)
    seedDocuments(2)
    engine.createIndex(DOC_V2, [:], leftoverMappings, [] as Set)
    engine.index(DOC_V2, 'stale', [id: 'stale'], IndexOptions.defaults())
    engine.clearLog()

    when:
    def result = migrator(TITLE_KEYWORD).bootstrap(Document)

    then:
    result.ok()
    engine.log().indexOf("deleteIndex ${DOC_V2}".toString()) < engine.log().findIndexOf { it.startsWith("createIndex ${DOC_V2}") }
    engine.log().contains("deleteIndex ${DOC_V2}".toString())
    engine.documents(DOC_V2).keySet() == ['d0', 'd1'] as Set
    engine.getMappings('documents')._meta.elastic_protos.hash == TITLE_KEYWORD.map(Document).contentHash()

    where:
    label        | leftoverMappings
    'wrong hash' | DEFAULT.map(Document).mappings()
    'no hash'    | [dynamic: 'false', properties: [id: [type: 'keyword']]]
  }

  def "a target index the alias already points at is never reused or deleted"() {
    given: 'a plan made before another process moved the alias to the target name'
    migrator().bootstrap(Document)
    def plan = migrator(TITLE_KEYWORD).plan(Document)
    engine.createIndex(DOC_V2, [:], TITLE_KEYWORD.map(Document).mappings(), [] as Set)
    engine.updateAliases([AliasAction.remove(DOC_V1, 'documents'), AliasAction.add(DOC_V2, 'documents')])
    engine.clearLog()

    when:
    def result = migrator(TITLE_KEYWORD).apply(plan)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
    result.outcomes()[0].error().message.contains("already points at ${DOC_V2}")
    engine.indices() == [(DOC_V1): [] as Set, (DOC_V2): ['documents'] as Set]
    !engine.log().any { it.startsWith('deleteIndex') || it.startsWith('setWriteBlock') }
  }

  def "a failed reindex keeps a target index it reused rather than created"() {
    given:
    def failing = new InMemorySearchEngine() {
      @Override
      ReindexResult reindex(ReindexRequest request) {
        throw new EngineException(EngineException.Kind.TIMEOUT, 0, 'took too long')
      }
    }
    copyInto(failing)
    failing.createIndex(DOC_V2, [:], TITLE_KEYWORD.map(Document).mappings(), [] as Set)

    when:
    def result = migrator(TITLE_KEYWORD, MigrationOptions.standard(), failing).bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
    failing.indices().keySet() == [DOC_V1, DOC_V2] as Set
    !failing.isWriteBlocked(DOC_V1)
  }

  def "a failed reindex never deletes the index the alias points at"() {
    given: 'the reindex fails after the alias was moved to the new index behind our back'
    def racing = new InMemorySearchEngine() {
      @Override
      ReindexResult reindex(ReindexRequest request) {
        super.reindex(request)
        updateAliases([AliasAction.remove(request.source(), 'documents'), AliasAction.add(request.dest(), 'documents')])
        throw new EngineException(EngineException.Kind.IO, 0, 'connection reset')
      }
    }
    copyInto(racing)

    when:
    def result = migrator(TITLE_KEYWORD, MigrationOptions.standard(), racing).bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
    racing.indices()[DOC_V2] == ['documents'] as Set
    !racing.log().contains("deleteIndex ${DOC_V2}".toString())
  }

  def "a failed reindex keeps the new index when the alias cannot be read again"() {
    given:
    def blind = new InMemorySearchEngine() {
      boolean reindexed

      @Override
      ReindexResult reindex(ReindexRequest request) {
        reindexed = true
        throw new EngineException(EngineException.Kind.IO, 0, 'connection reset')
      }

      @Override
      Map<String, Set<String>> getAliases(String aliasOrIndex) {
        if (reindexed) {
          throw new EngineException(EngineException.Kind.IO, 0, 'still down')
        }
        super.getAliases(aliasOrIndex)
      }
    }
    copyInto(blind)

    when:
    def result = migrator(TITLE_KEYWORD, MigrationOptions.standard(), blind).bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
    blind.indices().containsKey(DOC_V2)
    !blind.isWriteBlocked(DOC_V1)
  }

  def "a leftover target index is not touched when the alias cannot be read"() {
    given:
    def blind = new InMemorySearchEngine() {
      boolean planned

      @Override
      Map<String, Set<String>> getAliases(String aliasOrIndex) {
        if (planned) {
          throw new EngineException(EngineException.Kind.IO, 0, 'down')
        }
        super.getAliases(aliasOrIndex)
      }
    }
    copyInto(blind)
    blind.createIndex(DOC_V2, [:], DEFAULT.map(Document).mappings(), [] as Set)
    def migrator = migrator(TITLE_KEYWORD, MigrationOptions.standard(), blind)
    def plan = migrator.plan(Document)
    blind.planned = true

    when:
    def result = migrator.apply(plan)

    then:
    result.outcomes()[0].error().message.contains('could not be read')
    blind.indices().keySet() == [DOC_V1, DOC_V2] as Set
  }

  def "an alias swap that reports a failure but took effect counts as applied"() {
    given:
    def flaky = new InMemorySearchEngine() {
      @Override
      void updateAliases(List<AliasAction> actions) {
        super.updateAliases(actions)
        throw new EngineException(EngineException.Kind.IO, 0, 'response lost')
      }
    }
    copyInto(flaky)

    when:
    def result = migrator(TITLE_KEYWORD, MigrationOptions.standard(), flaky).bootstrap(Document)

    then:
    result.ok()
    result.outcomes()[0].status() == MigrationResult.Status.APPLIED
    result.outcomes()[0].index() == DOC_V2
    result.outcomes()[0].retainedIndices() == [DOC_V1]
    flaky.indices() == [(DOC_V1): [] as Set, (DOC_V2): ['documents'] as Set]
    flaky.documents(DOC_V2).size() == 3
  }

  def "a stale write block on the alias target is reported and cleared when no reindex is needed: #action"() {
    given:
    migrator(first).bootstrap(Document)
    def current = first.map(Document).name()
    engine.setWriteBlock(current, true)

    when:
    def plan = migrator().plan(Document)

    then:
    plan.items()[0].action() == action
    plan.items()[0].writeBlocked()
    plan.items()[0].warnings().any { it.contains("index ${current} has a write block") && it.contains('it will be cleared') }

    when:
    def result = migrator().apply(plan)

    then:
    result.ok()
    result.outcomes()[0].status() == MigrationResult.Status.APPLIED
    !engine.isWriteBlocked(current)
    engine.index('documents', 'after', [id: 'after'], IndexOptions.defaults()).index() == current

    where:
    first        | action
    DEFAULT      | Action.NO_OP
    NO_RELEVANCE | Action.PUT_MAPPING
  }

  def "with clearStaleWriteBlock off a write block is reported and left in place"() {
    given:
    migrator().bootstrap(Document)
    engine.setWriteBlock(DOC_V1, true)
    def options = MigrationOptions.standard().withClearStaleWriteBlock(false)

    when:
    def result = migrator(DEFAULT, options).bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.SKIPPED
    result.outcomes()[0].plan().warnings().any { it.contains('left in place (clearStaleWriteBlock is false)') }
    engine.isWriteBlocked(DOC_V1)
  }

  def "a write block on an index that is reindexed or blocked is reported, not cleared: #label"() {
    given:
    migrator().bootstrap(Document)
    seedDocuments(1)
    engine.setWriteBlock(DOC_V1, true)

    when:
    def result = migrator(TITLE_KEYWORD, options).bootstrap(Document)

    then:
    result.outcomes()[0].plan().action() == action
    result.outcomes()[0].plan().warnings().any { it.contains("index ${DOC_V1} has a write block") && it.contains(text) }
    engine.isWriteBlocked(DOC_V1)
    engine.indices()[(action == Action.REINDEX ? DOC_V2 : DOC_V1)] == ['documents'] as Set

    where:
    label     | options                                              | action         | text
    'reindex' | MigrationOptions.standard()                          | Action.REINDEX | 'moves the alias to an unblocked index'
    'blocked' | MigrationOptions.standard().withAllowReindex(false) | Action.BLOCKED | 'fail until it is cleared'
  }

  def "a dry run reports a stale write block without clearing it"() {
    given:
    migrator().bootstrap(Document)
    engine.setWriteBlock(DOC_V1, true)

    when:
    def result = migrator(DEFAULT, MigrationOptions.standard().withDryRun(true)).bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.DRY_RUN
    engine.isWriteBlocked(DOC_V1)
  }

  def "a migration script from the classpath is applied during the reindex"() {
    given: 'a prefixed alias so the script under migrations/scripted-documents applies only here'
    def naming = IndexNamingStrategy.prefixed('scripted')
    def v1 = ProtoMapper.builder().naming(naming).build()
    def v2 = ProtoMapper.builder().naming(naming).overrides(MappingOverrides.builder()
        .forPath('title') { it.setType(EsType.KEYWORD) }.build()).build()
    migrator(v1).bootstrap(Document)
    engine.index('scripted-documents', 'd1', [id: 'd1', title: 'T', body: 'old'], IndexOptions.defaults())

    expect: 'the script file is named after both hashes'
    v1.map(Document).contentHash() == DEFAULT.map(Document).contentHash()
    new ClasspathScriptProvider().script('scripted-documents', v1.map(Document).contentHash(),
        v2.map(Document).contentHash()).isPresent()

    when:
    def result = migrator(v2).bootstrap(Document)

    then:
    result.ok()
    engine.documents(v2.map(Document).name())['d1'] == [id: 'd1', title: 'T', firm_id: 'scripted-documents']
  }

  def "a failed reindex unblocks the old index, deletes the new one and reports MigrationException"() {
    given:
    def failing = new InMemorySearchEngine() {
      @Override
      ReindexResult reindex(ReindexRequest request) {
        throw new EngineException(EngineException.Kind.TIMEOUT, 0, 'took too long')
      }
    }
    copyInto(failing)

    when:
    def result = migrator(TITLE_KEYWORD, MigrationOptions.standard(), failing).bootstrap(Document)

    then:
    !result.ok()
    def outcome = result.outcomes()[0]
    outcome.status() == MigrationResult.Status.FAILED
    outcome.error() instanceof MigrationException
    outcome.error().cause instanceof EngineException
    outcome.error().message.contains('took too long')
    outcome.index() == DOC_V1
    failing.indices() == [(DOC_V1): ['documents'] as Set]
    !failing.isWriteBlocked(DOC_V1)
    result.problems() == [outcome]
  }

  def "reindex results with failures, timeouts or missing documents fail the migration"() {
    given:
    def broken = new CannedReindexEngine(answer: answer, dropOne: mode == 'count')
    copyInto(broken)

    when:
    def result = migrator(TITLE_KEYWORD, MigrationOptions.standard(), broken).bootstrap(Document)

    then:
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
    result.outcomes()[0].error() instanceof MigrationException
    result.outcomes()[0].error().message.contains(message)
    broken.indices().keySet() == [DOC_V1] as Set
    !broken.isWriteBlocked(DOC_V1)

    where:
    mode       | answer                                                  | message
    'failures' | new ReindexResult(3, 2, 0, [[cause: 'bad']], 1, false) | '1 failures'
    'timeout'  | new ReindexResult(3, 3, 0, [], 1, true)                | 'timed out'
    'count'    | new ReindexResult(3, 3, 0, [], 1, false)               | 'copied 2 of 3'
  }

  def "a failed alias swap and failing cleanup still report the original problem"() {
    given:
    def stubborn = new InMemorySearchEngine() {
      @Override
      void updateAliases(List<AliasAction> actions) {
        throw new EngineException(EngineException.Kind.OTHER, 500, 'swap failed')
      }

      @Override
      void deleteIndex(String index) {
        throw new EngineException(EngineException.Kind.OTHER, 500, 'delete failed')
      }

      @Override
      void setWriteBlock(String index, boolean blocked) {
        if (!blocked) {
          throw new EngineException(EngineException.Kind.OTHER, 500, 'unblock failed')
        }
        super.setWriteBlock(index, blocked)
      }
    }
    copyInto(stubborn)

    when:
    def result = migrator(TITLE_KEYWORD, MigrationOptions.standard(), stubborn).bootstrap(Document)

    then:
    result.outcomes()[0].error() instanceof MigrationException
    result.outcomes()[0].error().message.contains('alias swap')
    stubborn.indices().containsKey(DOC_V2)
  }

  def "a non-reindex failure is reported with the engine's exception"() {
    given: 'an index with the target name exists without the alias'
    engine.createIndex(DOC_V1, [:], DEFAULT.map(Document).mappings(), [] as Set)

    when:
    def result = migrator().bootstrap(Document)

    then:
    result.outcomes()[0].plan().action() == Action.CREATE
    result.outcomes()[0].status() == MigrationResult.Status.FAILED
    result.outcomes()[0].error() instanceof EngineException
  }

  def "removed fields are kept with a warning or reindexed, depending on the policy"() {
    given:
    migrator().bootstrap(Document)

    when:
    def plan = migrator(NO_RELEVANCE, MigrationOptions.standard().withRemovedFields(policy)).plan(Document)

    then:
    plan.items()[0].action() == action
    plan.items()[0].diff().removed()*.path == ['relevance']
    plan.items()[0].warnings().any { it.contains('relevance') }
    plan.items()[0].reason() == reason

    where:
    policy                     | action         | reason
    RemovedFieldPolicy.KEEP    | Action.NO_OP   | "mappings of ${DOC_V1} already match".toString()
    RemovedFieldPolicy.REINDEX | Action.REINDEX | 'breaking change: removed [relevance]'
  }

  def "a concrete index named like the alias is blocked"() {
    given:
    engine.createIndex('documents', [:], [:], [] as Set)

    when:
    def plan = migrator().plan(Document)
    def result = migrator().apply(plan)

    then:
    plan.items()[0].action() == Action.BLOCKED
    plan.items()[0].reason().contains('a concrete index named like the alias exists')
    plan.blocked() == plan.items()
    result.outcomes()[0].status() == MigrationResult.Status.SKIPPED
    !result.ok()
  }

  def "an engine that reports the alias name only through indexExists is blocked too"() {
    given:
    SearchEngine stub = Stub() {
      getAliases('documents') >> ['other-v1': ['other'] as Set]
      indexExists('documents') >> true
    }

    expect:
    migrator(DEFAULT, MigrationOptions.standard(), stub).plan(Document).items()[0].action() == Action.BLOCKED
  }

  def "an alias on two indices is blocked"() {
    given:
    engine.createIndex('documents-a', [:], [:], ['documents'] as Set)
    engine.createIndex('documents-b', [:], [:], ['documents'] as Set)

    when:
    def item = migrator().plan(Document).items()[0]

    then:
    item.action() == Action.BLOCKED
    item.reason().contains('2 indices')
  }

  def "a breaking change is blocked when reindexing is not allowed"() {
    given:
    migrator().bootstrap(Document)

    when:
    def item = migrator(TITLE_KEYWORD, MigrationOptions.standard().withAllowReindex(false))
        .plan(Document).items()[0]

    then:
    item.action() == Action.BLOCKED
    item.reason().contains('allowReindex is false')
    item.reason().contains('title type text -> keyword')
    item.currentIndex() == DOC_V1
  }

  def "an index that already has the target name but other mappings is blocked"() {
    given:
    engine.createIndex(DOC_V1, [:], TITLE_KEYWORD.map(Document).mappings(), ['documents'] as Set)

    when:
    def item = migrator().plan(Document).items()[0]

    then:
    item.action() == Action.BLOCKED
    item.reason().contains('already has the target name')
  }

  def "settings differences are warnings, never changes"() {
    given:
    engine.createIndex(DEFAULT.map(Firm).name(), [number_of_shards: 2],
        DEFAULT.map(Firm).mappings(), ['firms'] as Set)
    def mapper = ProtoMapper.builder()
        .overrides(MappingOverrides.builder().setting('refresh_interval', '5s').build()).build()

    when:
    def item = migrator(mapper).plan(Firm).items()[0]

    then:
    item.action() == Action.NO_OP
    item.diff().settingsChanged()
    !item.diff().isEmpty()
    item.warnings() == ['number_of_shards is 2 but the proto asks for 1; not applied',
                        'settings differ; not applied']
  }

  def "a legacy index without metadata reports the legacy hash"() {
    given:
    Map mappings = new TreeMap(DEFAULT.map(Document).mappings())
    mappings.remove('_meta')
    engine.createIndex('documents-old', [:], mappings, ['documents'] as Set)

    when:
    def item = migrator().plan(Document).items()[0]

    then:
    item.currentHash() == 'legacy'
    item.action() == Action.NO_OP
    item.reason() == 'mappings of documents-old already match'
    item.currentIndex() == 'documents-old'
  }

  def "FAIL_FAST stops at the first failure, CONTINUE attempts everything"() {
    given:
    engine.createIndex(DOC_V1, [:], [:], [] as Set)

    when:
    def result = migrator(DEFAULT, MigrationOptions.standard().withFailure(policy)).bootstrap(Document, Firm)

    then:
    result.outcomes()*.status == statuses
    engine.indexExists('firms') == firmCreated

    where:
    policy                   | statuses                                                       | firmCreated
    FailurePolicy.FAIL_FAST  | [MigrationResult.Status.FAILED]                                | false
    FailurePolicy.CONTINUE   | [MigrationResult.Status.FAILED, MigrationResult.Status.APPLIED] | true
  }

  def "a blocked item does not stop FAIL_FAST"() {
    given:
    engine.createIndex('documents', [:], [:], [] as Set)

    when:
    def result = migrator().bootstrap(Document, Firm)

    then:
    result.outcomes()*.status == [MigrationResult.Status.SKIPPED, MigrationResult.Status.APPLIED]
    !result.ok()
  }

  def "a dry run changes nothing"() {
    given:
    migrator().bootstrap(Document)
    engine.clearLog()
    def options = MigrationOptions.standard().withDryRun(true)

    when:
    def result = migrator(TITLE_KEYWORD, options).bootstrap(Firm, Document)

    then:
    result.ok()
    result.outcomes()*.status == [MigrationResult.Status.DRY_RUN] * 2
    result.outcomes()*.index == [DEFAULT.map(Firm).name(), DOC_V2]
    engine.log().every { it.startsWith('get') || it.startsWith('indexExists') }
    engine.indices().keySet() == [DOC_V1] as Set
  }

  def "a dry run of a no-op reports the current index"() {
    given:
    migrator().bootstrap(Document)

    expect:
    migrator(DEFAULT, MigrationOptions.standard().withDryRun(true)).bootstrap(Document)
        .outcomes()[0].index() == DOC_V1
  }

  def "the plan renders as a report and as JSON"() {
    given:
    migrator().bootstrap(Document)
    engine.createIndex('firms', [:], [:], [] as Set)

    when:
    def plan = migrator(TITLE_KEYWORD).plan(Document, Firm)
    String report = plan.toReport()
    Map json = Json.parseObject(plan.toJson())

    then:
    report.readLines()[0] == 'Migration plan: 2 indices, changes pending'
    report.contains("REINDEX     documents: ${DOC_V1} -> ${DOC_V2} (added 0, removed 0, changed 1)")
    report.contains("BLOCKED     firms: (none) -> ${DEFAULT.map(Firm).name()}")
    report.contains('blocked: a concrete index named like the alias exists: firms')
    !report.contains('(in place)')
    json.hasChanges == true
    json.items*.action == ['REINDEX', 'BLOCKED']
    json.items[0].currentIndex == DOC_V1
    json.items[0].targetIndex == DOC_V2
    json.items[0].targetHash == TITLE_KEYWORD.map(Document).contentHash()
    json.items[0].diff.typeChanged == [[path: 'title', before: 'text', after: 'keyword']]
    json.items[0].diff.settingsChanged == false
    json.items[1].currentIndex == null
  }

  def "a single-item report says index and shows warnings"() {
    given:
    migrator().bootstrap(Document)

    when:
    String report = migrator(NO_RELEVANCE).plan(Document).toReport()

    then:
    report.startsWith('Migration plan: 1 index, no changes')
    report.contains("NO_OP       documents: ${DOC_V1} (in place) (added 0, removed 1, changed 0)")
    report.contains('warning: fields no longer in the proto: [relevance] (kept in the mapping)')
    report.contains("mappings of ${DOC_V1} already match")
  }

  def "the report shows creations as current -> target and in-place updates without an arrow"() {
    given:
    migrator(NO_RELEVANCE).bootstrap(Document)
    String original = NO_RELEVANCE.map(Document).name()

    when:
    String report = migrator().plan(Document, Firm).toReport()

    then:
    report.contains("PUT_MAPPING documents: ${original} (in place) (added 1, removed 0, changed 0)")
    report.contains("CREATE      firms: (none) -> ${DEFAULT.map(Firm).name()} (added 0, removed 0, changed 0)")
  }

  def "the builder requires an engine"() {
    when:
    IndexMigrator.builder().build()

    then:
    thrown(NullPointerException)

    expect:
    IndexMigrator.builder().engine(engine).scriptProvider(MigrationScriptProvider.none()).build()
        .options() == MigrationOptions.standard()
  }

  /** Copies the default Document setup (index v1 with three documents) into another engine. */
  private static void copyInto(InMemorySearchEngine target) {
    target.createIndex(DOC_V1, [number_of_shards: 1], DEFAULT.map(Document).mappings(),
        ['documents'] as Set)
    3.times { i ->
      target.index('documents', "d${i}", [id: "d${i}".toString()], IndexOptions.defaults())
    }
    target.clearLog()
  }

  /** Copies like the real engine, then answers with a canned result, optionally losing a document. */
  static class CannedReindexEngine extends InMemorySearchEngine {
    ReindexResult answer
    boolean dropOne

    @Override
    ReindexResult reindex(ReindexRequest request) {
      super.reindex(request)
      if (dropOne) {
        delete(request.dest(), 'd0')
      }
      return answer
    }
  }
}
