package com.phatjam98.elasticprotos.testkit

import com.phatjam98.elasticprotos.engine.AliasAction
import com.phatjam98.elasticprotos.engine.BulkOp
import com.phatjam98.elasticprotos.engine.EngineException
import com.phatjam98.elasticprotos.engine.EngineInfo
import com.phatjam98.elasticprotos.engine.EngineProduct
import com.phatjam98.elasticprotos.engine.IndexOptions
import com.phatjam98.elasticprotos.engine.ReindexRequest
import com.phatjam98.elasticprotos.engine.SearchResult
import com.phatjam98.elasticprotos.engine.UpdateByQueryRequest
import com.phatjam98.elasticprotos.proto.SortingOrder
import com.phatjam98.elasticprotos.proto.TermsAggregation
import com.phatjam98.elasticprotos.proto.TermsOrder
import com.phatjam98.elasticprotos.proto.TermsOrderBy
import com.phatjam98.elasticprotos.query.AggregationResults
import com.phatjam98.elasticprotos.query.Aggs
import com.phatjam98.elasticprotos.query.Criteria
import java.time.Duration
import spock.lang.Specification

class InMemorySearchEngineSpec extends Specification {

  static final Map MAPPINGS = [
      dynamic   : 'strict',
      _meta     : [elastic_protos: [hash: 'aaaa']],
      properties: [
          title : [type: 'text'],
          status: [type: 'keyword'],
          owner : [type: 'object', properties: [name: [type: 'keyword']]],
      ],
  ]

  InMemorySearchEngine engine = new InMemorySearchEngine()

  def "resolves aliases like a real engine"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.createIndex('a-v2', [:], MAPPINGS, [] as Set)

    expect:
    engine.indexExists('a')
    engine.indexExists('a-v2')
    !engine.indexExists('missing')
    engine.getAliases('a') == ['a-v1': ['a'] as Set]
    engine.getAliases('a-v2') == ['a-v2': [] as Set]
    engine.getAliases('missing') == [:]
    engine.getMappings('a').properties.title == [type: 'text']

    when: 'the alias spans two indices'
    engine.updateAliases([AliasAction.add('a-v2', 'a')])
    engine.getMappings('a')

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.CONFLICT
    engine.getAliases('a').keySet() == ['a-v1', 'a-v2'] as Set

    when:
    engine.getSettings('missing')

    then:
    thrown(EngineException)
  }

  def "returns mappings and settings in engine form"() {
    given:
    engine.createIndex('a-v1', [number_of_shards: 2, analysis: [analyzer: [x: [type: 'standard']]]],
        MAPPINGS + [dynamic: false], [] as Set)

    when:
    Map mappings = engine.getMappings('a-v1')
    Map settings = engine.getSettings('a-v1')

    then:
    mappings.dynamic == 'false'
    mappings.properties.owner == [properties: [name: [type: 'keyword']]]
    mappings._meta == [elastic_protos: [hash: 'aaaa']]
    settings == [index: [number_of_shards: '2', number_of_replicas: '1',
                         analysis: [analyzer: [x: [type: 'standard']]]]]
  }

  def "create rejects duplicates and alias name clashes"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)

    when:
    engine.createIndex(name, [:], MAPPINGS, aliases as Set)

    then:
    def e = thrown(EngineException)
    e.kind == kind

    where:
    name   | aliases  | kind
    'a-v1' | []       | EngineException.Kind.ALREADY_EXISTS
    'a'    | []       | EngineException.Kind.ALREADY_EXISTS
    'b-v1' | ['a-v1'] | EngineException.Kind.BAD_REQUEST
  }

  def "putMappings deep-merges and rejects type changes unless told not to"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, [] as Set)

    when:
    engine.putMappings('a-v1', [_meta: [elastic_protos: [hash: 'bbbb']],
                                properties: [owner: [type: 'object', properties: [age: [type: 'integer']]],
                                             extra: [type: 'keyword']]])

    then:
    Map mappings = engine.getMappings('a-v1')
    mappings._meta.elastic_protos.hash == 'bbbb'
    mappings.properties.owner.properties.keySet() == ['age', 'name'] as Set
    mappings.properties.extra == [type: 'keyword']

    when:
    engine.putMappings('a-v1', [properties: [owner: [properties: [name: [type: 'text']]]]])

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.BAD_REQUEST
    e.message.contains('owner.name')

    when:
    engine.rejectTypeChanges(false).putMappings('a-v1', [properties: [title: [type: 'keyword']]])

    then:
    engine.getMappings('a-v1').properties.title.type == 'keyword'
  }

  def "updateAliases is atomic"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.createIndex('a-v2', [:], MAPPINGS, [] as Set)

    when: 'the second action is invalid'
    engine.updateAliases([AliasAction.remove('a-v1', 'a'), AliasAction.add('nope', 'a')])

    then:
    thrown(EngineException)
    engine.indices() == ['a-v1': ['a'] as Set, 'a-v2': [] as Set]

    when: 'removing an alias that is not there'
    engine.updateAliases([AliasAction.add('a-v2', 'a'), AliasAction.remove('a-v2', 'b')])

    then:
    thrown(EngineException)
    engine.indices()['a-v2'].isEmpty()

    when: 'adding an alias named like an index'
    engine.updateAliases([AliasAction.add('a-v2', 'a-v1')])

    then:
    thrown(EngineException)

    when:
    engine.updateAliases([AliasAction.remove('a-v1', 'a'), AliasAction.add('a-v2', 'a')])

    then:
    engine.indices() == ['a-v1': [] as Set, 'a-v2': ['a'] as Set]
    engine.log().last() == 'updateAliases [remove a-v1 a, add a-v2 a]'

    when:
    engine.updateAliases([AliasAction.removeIndex('a-v1')])

    then:
    engine.indices().keySet() == ['a-v2'] as Set
  }

  def "write-blocked indices refuse writes with 403"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.index('a', '1', [title: 'x'], IndexOptions.defaults())
    engine.setWriteBlock('a-v1', true)

    when:
    action.call(engine)

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.BLOCKED
    e.status == 403
    e.message.contains('blocked')
    engine.isWriteBlocked('a-v1')
    engine.getSettings('a-v1').index.blocks == [write: 'true']
    engine.log().contains('setWriteBlock a-v1=true')

    where:
    action << [
        { InMemorySearchEngine it -> it.index('a', '2', [title: 'y'], IndexOptions.defaults()) },
        { InMemorySearchEngine it -> it.update('a', '1', [title: 'y']) },
        { InMemorySearchEngine it -> it.delete('a', '1') },
        { InMemorySearchEngine it -> it.bulk([BulkOp.index('a', '3', [title: 'z'])]) },
    ]
  }

  def "unblocking allows writes again"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, [] as Set)
    engine.setWriteBlock('a-v1', true)
    engine.setWriteBlock('a-v1', false)

    when:
    def result = engine.index('a-v1', null, [title: 'x'], IndexOptions.refreshed())

    then:
    result.result() == 'created'
    result.id() == 'generated-1'
    !engine.isWriteBlocked('a-v1')
  }

  def "documents can be written, updated, deleted and bulk-loaded"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)

    when:
    def first = engine.index('a', '1', [title: 'x', status: 'open'], IndexOptions.defaults())
    def second = engine.index('a', '1', [title: 'x', status: 'open'], IndexOptions.defaults())
    def updated = engine.update('a', '1', [status: 'closed'])
    def bulk = engine.bulk([
        BulkOp.create('a', '2', [title: 'b']),
        BulkOp.create('a', '2', [title: 'b']),
        BulkOp.update('a', '9', [title: 'c']),
        BulkOp.update('a', '2', [status: 'open']),
        BulkOp.index('a', null, [title: 'd']),
        BulkOp.delete('a', '8'),
    ])

    then:
    first.result() == 'created'
    second.result() == 'updated'
    second.version() == 2
    updated.version() == 3
    engine.documents('a-v1')['1'] == [title: 'x', status: 'closed']
    bulk.errors()
    bulk.items()*.status == [201, 409, 404, 200, 201, 404]
    bulk.failures().size() == 2
    engine.documents('a-v1')['2'] == [title: 'b', status: 'open']

    when:
    engine.index('a', '1', [title: 'again'], new IndexOptions(true, IndexOptions.Refresh.NONE))

    then:
    def conflict = thrown(EngineException)
    conflict.status == 409

    when:
    engine.update('a', 'missing', [title: 'x'])

    then:
    thrown(EngineException)

    expect:
    engine.delete('a', '1')
    !engine.delete('a', '1')
  }

  def "get reads one document by id through an alias, and missing ids or indices are empty"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.index('a', '1', [title: 'x'], IndexOptions.defaults())

    when:
    def hit = engine.get('a', '1')

    then:
    hit.present
    hit.get().index() == 'a-v1'
    hit.get().id() == '1'
    hit.get().score() == null
    hit.get().sort() == []
    hit.get().source() == [title: 'x']
    !engine.get('a', '2').present
    !engine.get('missing', '1').present
    engine.log().contains('get a/1')

    when:
    engine.delete('a', '1', IndexOptions.refreshed())

    then:
    !engine.get('a-v1', '1').present

    when:
    engine.createIndex('a-v2', [:], MAPPINGS, ['a'] as Set)
    engine.get('a', '1')

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.BAD_REQUEST
  }

  def "writes through an alias on two indices are refused"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.createIndex('a-v2', [:], MAPPINGS, ['a'] as Set)

    when:
    engine.index('a', '1', [:], IndexOptions.defaults())

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.BAD_REQUEST
  }

  def "reindex copies documents and applies the script stand-in"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.createIndex('a-v2', [:], MAPPINGS, [] as Set)
    engine.index('a', '1', [title: 'one', status: 'open'], IndexOptions.defaults())
    engine.index('a', '2', [title: 'two', status: 'closed'], IndexOptions.defaults())

    when:
    def result = engine.reindex(ReindexRequest.of('a', 'a-v2').withScript('''
        ctx._source.remove('status');
        ctx._source.migrated = params.to_hash
        ''', [to_hash: 'bbbb']))

    then:
    result.ok()
    result.total() == 2
    result.created() == 2
    engine.documents('a-v2') == ['1': [title: 'one', migrated: 'bbbb'],
                                 '2': [title: 'two', migrated: 'bbbb']]
    engine.documents('a-v1')['1'].status == 'open'

    when: 'again, overwriting'
    def again = engine.reindex(ReindexRequest.of('a-v1', 'a-v2'))

    then:
    again.updated() == 2
    engine.log().contains('reindex a-v1 -> a-v2')
  }

  def "reindex rejects unsupported scripts and missing indices"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, [] as Set)
    engine.createIndex('a-v2', [:], MAPPINGS, [] as Set)

    when:
    engine.reindex(request)

    then:
    def e = thrown(EngineException)
    e.kind == kind

    where:
    request                                                                    | kind
    ReindexRequest.of('a-v1', 'a-v2').withScript('ctx._source.x += 1', [:])   | EngineException.Kind.OTHER
    ReindexRequest.of('missing', 'a-v2')                                       | EngineException.Kind.NOT_FOUND
    ReindexRequest.of('a-v1', 'missing')                                       | EngineException.Kind.NOT_FOUND
  }

  def "search supports match_all, term, paging and total hits"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.createIndex('b-v1', [:], MAPPINGS, ['a'] as Set)
    engine.index('a-v1', '1', [status: 'open', tags: ['x', 'y']], IndexOptions.defaults())
    engine.index('a-v1', '2', [status: 'closed'], IndexOptions.defaults())
    engine.index('b-v1', '3', [status: 'open'], IndexOptions.defaults())
    engine.refresh('a')

    expect:
    engine.search(['a'], [size: 0, track_total_hits: true]).total() == 3
    engine.search(['a'], [size: 0, track_total_hits: true]).hits().isEmpty()
    engine.search(['a-v1'], [query: [match_all: [:]]]).hits()*.id() == ['1', '2']
    engine.search(['a'], [query: [term: [status: 'open']]]).hits()*.id() == ['1', '3']
    engine.search(['a'], [query: [term: [status: [value: 'closed']]]]).hits()*.id() == ['2']
    engine.search(['a'], [query: [term: [tags: 'y']]]).hits()*.id() == ['1']
    engine.search(['a'], [from: 1, size: 1]).hits()*.id() == ['2']
    !engine.search(['a'], [track_total_hits: false]).totalExact()
    engine.search(['a'], [track_total_hits: false]).total() == SearchResult.NO_TOTAL
    !engine.search(['a'], [track_total_hits: false]).hasTotal()

    when:
    engine.search(['a'], [query: [range: [n: [gte: 1]]]])

    then:
    thrown(EngineException)

    when:
    engine.search(['missing'], [:])

    then:
    thrown(EngineException)
  }

  def "search supports ids, terms, dotted paths and bool clauses"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.index('a', '1', [status: 'open', owner: [city: 'Reno'], offices: [[city: 'Reno'], [city: 'Elko']]], IndexOptions.defaults())
    engine.index('a', '2', [status: 'closed', owner: [city: 'Vegas']], IndexOptions.defaults())
    engine.index('a', '3', [status: 'open', owner: [city: 'Vegas']], IndexOptions.defaults())

    expect:
    engine.search(['a'], [query: [ids: [values: ['3', '1', 'x']]]]).hits()*.id() == ['1', '3']
    engine.search(['a'], [query: [terms: [status: ['closed', 'other']]]]).hits()*.id() == ['2']
    engine.search(['a'], [query: [term: ['owner.city': 'Vegas']]]).hits()*.id() == ['2', '3']
    engine.search(['a'], [query: [term: ['offices.city': 'Elko']]]).hits()*.id() == ['1']
    engine.search(['a'], [query: [bool: [filter: [[term: [status: 'open']]]]]]).hits()*.id() == ['1', '3']
    engine.search(['a'], [query: [bool: [filter: [term: [status: 'open']], must: [[term: ['owner.city': 'Vegas']]]]]]).hits()*.id() == ['3']
    engine.search(['a'], [query: [bool: [must_not: [[term: [status: 'open']]]]]]).hits()*.id() == ['2']
    engine.search(['a'], [query: [bool: [should: [[ids: [values: ['1']]], [term: [status: 'closed']]], minimum_should_match: 1]]]).hits()*.id() == ['1', '2']
    engine.search(['a'], [query: [bool: [should: [[ids: [values: ['1']]]]]]]).hits()*.id() == ['1']
    engine.search(['a'], [query: [bool: [filter: [[match_all: [:]]], should: [[ids: [values: ['1']]]]]]]).hits()*.id() == ['1', '2', '3']
    engine.search(['a'], [query: [bool: [:]]]).hits().size() == 3

    when:
    engine.search(['a'], [query: [bool: [boost: 2]]])

    then:
    thrown(EngineException)

    when:
    engine.search(['a'], [query: [term: [status: 'open'], ids: [values: []]]])

    then:
    thrown(EngineException)

    when:
    engine.search(['a'], [query: [terms: [status: ['open'], owner: ['x']]]])

    then:
    thrown(EngineException)
  }

  def "search_after resumes after the position carried in the sort values"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    (1..5).each { engine.index('a', "$it", [n: it], IndexOptions.defaults()) }

    when:
    def first = engine.search(['a'], [size: 2, sort: [[_shard_doc: 'asc']]])
    def second = engine.search(['a'], [size: 2, sort: [[_shard_doc: 'asc']], search_after: first.lastSort()])
    def last = engine.search(['a'], [size: 2, sort: [[_shard_doc: 'asc']], search_after: [4L]])

    then:
    first.hits()*.id() == ['1', '2']
    first.lastSort() == [1L]
    second.hits()*.id() == ['3', '4']
    last.hits().isEmpty()
    engine.search(['a'], [size: 1]).hits()[0].sort() == []
  }

  def "points in time pin the searched indices"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.index('a', '1', [status: 'open'], IndexOptions.defaults())

    when:
    def pit = engine.openPit(['a'], Duration.ofMinutes(1))
    def result = engine.search([], [pit: [id: pit.id()]])

    then:
    UUID.fromString(pit.id())
    result.pitId() == pit.id()
    result.hits()*.index() == ['a-v1']

    when:
    engine.closePit(pit)
    engine.search([], [pit: [id: pit.id()]])

    then:
    thrown(EngineException)
  }

  def "info and health are configurable"() {
    expect:
    engine.info() == new EngineInfo(EngineProduct.ELASTICSEARCH, '9.0.0', 'in-memory')
    engine.health().status() == 'green'
    engine.health().numberOfDataNodes() == 1

    when:
    engine.engineInfo(new EngineInfo(EngineProduct.OPENSEARCH, '3.0.0', 'os')).dataNodes(3)

    then:
    engine.info().product() == EngineProduct.OPENSEARCH
    engine.health().numberOfDataNodes() == 3
  }

  def "deleteIndex removes concrete indices only and the log can be cleared"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)

    when:
    engine.deleteIndex('a')

    then:
    thrown(EngineException)

    when:
    engine.deleteIndex('a-v1')
    engine.close()

    then:
    engine.indices().isEmpty()
    engine.log() == ['createIndex a-v1 aliases=[a]', 'deleteIndex a', 'deleteIndex a-v1', 'close']

    when:
    engine.clearLog()

    then:
    engine.log().isEmpty()
  }

  // ------------------------------------------------------------------ versions and sequence numbers

  def "every write takes the next global sequence number and bumps the document version"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.createIndex('b-v1', [:], MAPPINGS, [] as Set)

    when:
    def first = engine.index('a', '1', [title: 'x'], IndexOptions.defaults())
    def other = engine.index('b-v1', '1', [title: 'y'], IndexOptions.defaults())
    def second = engine.index('a', '1', [title: 'z'], IndexOptions.defaults())
    def updated = engine.update('a', '1', [status: 'open'])

    then:
    [first, other, second, updated]*.seqNo() == [0L, 1L, 2L, 3L]
    [first, other, second, updated]*.primaryTerm() == [1L, 1L, 1L, 1L]
    [first, other, second, updated]*.version() == [1L, 1L, 2L, 3L]

    when:
    def hit = engine.get('a', '1').get()

    then:
    hit.version() == 3L
    hit.seqNo() == 3L
    hit.primaryTerm() == 1L
  }

  def "the global checkpoint is the sequence number counter, lowered by a lag, or unknown"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, [] as Set)
    3.times { engine.index('a-v1', "${it}", [title: 'x'], IndexOptions.defaults()) }

    expect:
    engine.globalCheckpoint('a-v1') == OptionalLong.of(2)
    engine.seqNoCheckpoint('a-v1') == 2L
    engine.globalCheckpointLag(2).globalCheckpoint('a-v1') == OptionalLong.of(0)
    engine.globalCheckpoints(false).globalCheckpoint('a-v1') == OptionalLong.empty()
    engine.log().contains('globalCheckpoint a-v1')
  }

  def "a conditional index or delete succeeds only on the current sequence number and primary term"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    def written = engine.index('a', '1', [title: 'x'], IndexOptions.defaults())

    when:
    def next = engine.index('a', '1', [title: 'y'], IndexOptions.ifMatch(written.seqNo(), written.primaryTerm()))

    then:
    next.version() == 2L
    next.seqNo() > written.seqNo()

    when: 'the sequence number is stale'
    engine.index('a', '1', [title: 'z'], IndexOptions.ifMatch(written.seqNo(), 1))

    then:
    def stale = thrown(EngineException)
    stale.kind == EngineException.Kind.CONFLICT
    stale.status == 409
    stale.message.contains('current document has seqNo [' + next.seqNo() + ']')
    engine.documents('a-v1')['1'] == [title: 'y']

    when: 'the primary term is wrong'
    engine.delete('a', '1', IndexOptions.ifMatch(next.seqNo(), 2))

    then:
    thrown(EngineException)

    when: 'the document does not exist'
    engine.index('a', '9', [title: 'z'], IndexOptions.ifMatch(0, 1))

    then:
    def missing = thrown(EngineException)
    missing.kind == EngineException.Kind.CONFLICT
    missing.message.contains('no document was found')

    expect:
    engine.delete('a', '1', IndexOptions.ifMatch(next.seqNo(), next.primaryTerm()))
    !engine.get('a', '1').present
  }

  def "conditional bulk items that do not match fail with 409 and leave the document alone"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    def one = engine.index('a', '1', [title: 'one'], IndexOptions.defaults())
    def two = engine.index('a', '2', [title: 'two'], IndexOptions.defaults())

    when:
    def result = engine.bulk([
        BulkOp.indexIfMatch('a', '1', [title: 'uno'], one.seqNo(), 1),
        BulkOp.indexIfMatch('a', '2', [title: 'dos'], one.seqNo(), 1),
        BulkOp.deleteIfMatch('a', '2', two.seqNo() + 100, 1),
        BulkOp.deleteIfMatch('a', '1', 99, 1),
    ])

    then:
    result.errors()
    result.items()*.status() == [200, 409, 409, 409]
    result.failures()*.id() == ['2', '2', '1']
    result.failures()[0].error().startsWith('version_conflict_engine_exception')
    engine.documents('a-v1') == ['1': [title: 'uno'], '2': [title: 'two']]

    when:
    def hit = engine.get('a', '1').get()
    def deleted = engine.bulk([BulkOp.deleteIfMatch('a', '1', hit.seqNo(), hit.primaryTerm())])

    then:
    !deleted.errors()
    deleted.items()[0].status() == 200
    !engine.get('a', '1').present
  }

  def "search returns version metadata only when the body asks, and accepts a highlight"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.index('a', '1', [title: 'x'], IndexOptions.defaults())
    engine.index('a', '1', [title: 'x'], IndexOptions.defaults())

    when:
    def plain = engine.search(['a'], [:]).hits()[0]
    def versioned = engine.search(['a'], [version: true]).hits()[0]
    def full = engine.search(['a'], [version: true, seq_no_primary_term: true, sort: ['_doc'],
                                     highlight: [fields: [title: [:]]]]).hits()[0]

    then:
    plain.version() == null
    plain.seqNo() == null
    plain.primaryTerm() == null
    versioned.version() == 2L
    versioned.seqNo() == null
    full.version() == 2L
    full.seqNo() == 1L
    full.primaryTerm() == 1L
    full.sort() == [0L]
    full.highlight() == [:]
  }

  def "updateByQuery rewrites the matching documents and bumps their versions"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.index('a', '1', [title: 'one', status: 'open'], IndexOptions.defaults())
    engine.index('a', '2', [title: 'two', status: 'closed'], IndexOptions.defaults())
    engine.index('a', '3', [title: 'three', status: 'open'], IndexOptions.defaults())

    when:
    def all = engine.updateByQuery(UpdateByQueryRequest.of('a'))

    then:
    all.total() == 3
    all.updated() == 3
    all.accounted()
    all.ok()
    ['1', '2', '3'].collect { engine.get('a', it).get().version() } == [2L, 2L, 2L]
    engine.get('a', '3').get().seqNo() == 5L
    engine.documents('a-v1')['1'] == [title: 'one', status: 'open']

    when:
    def some = engine.updateByQuery(UpdateByQueryRequest.builder().index('a-v1')
        .query([ids: [values: ['1', '3']]]).proceedOnConflicts(true).build())
    def closed = engine.updateByQuery(UpdateByQueryRequest.of('a').withQuery([term: [status: 'closed']]))

    then:
    some.updated() == 2
    closed.updated() == 1
    ['1', '2', '3'].collect { engine.get('a', it).get().version() } == [3L, 3L, 3L]
    engine.log().contains('updateByQuery a-v1')

    when:
    engine.setWriteBlock('a-v1', true)
    engine.updateByQuery(UpdateByQueryRequest.of('a'))

    then:
    def blocked = thrown(EngineException)
    blocked.kind == EngineException.Kind.BLOCKED

    when:
    engine.updateByQuery(UpdateByQueryRequest.of('missing'))

    then:
    def missing = thrown(EngineException)
    missing.kind == EngineException.Kind.NOT_FOUND
  }

  def "reindex copies a snapshot taken when it starts and honours a source query"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.createIndex('a-v2', [:], MAPPINGS, [] as Set)
    engine.index('a', '1', [title: 'one'], IndexOptions.defaults())
    engine.index('a', '2', [title: 'two'], IndexOptions.defaults())
    engine.index('a', '3', [title: 'three'], IndexOptions.defaults())
    List<String> seen = []
    engine.duringReindex { request ->
      seen << request.dest()
      engine.index('a', '1', [title: 'changed'], IndexOptions.defaults())
      engine.index('a', '4', [title: 'late'], IndexOptions.defaults())
      engine.delete('a', '2')
    }

    when:
    def result = engine.reindex(ReindexRequest.of('a', 'a-v2').withQuery([ids: [values: ['1', '2', '4']]]))

    then:
    seen == ['a-v2']
    result.total() == 2
    result.created() == 2
    result.accounted()
    engine.documents('a-v2') == ['1': [title: 'one'], '2': [title: 'two']]
    engine.documents('a-v1').keySet() == ['1', '3', '4'] as Set

    when: 'the hook is removed'
    engine.duringReindex(null)
    def again = engine.reindex(ReindexRequest.of('a', 'a-v2'))

    then:
    again.total() == 3
    again.updated() == 1
    again.created() == 2
    engine.documents('a-v2')['1'] == [title: 'changed']
  }

  def "an external-version reindex keeps source versions and counts newer destination documents as conflicts"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.createIndex('a-v2', [:], MAPPINGS, [] as Set)
    engine.index('a', '1', [title: 'one'], IndexOptions.defaults())
    engine.index('a', '1', [title: 'one again'], IndexOptions.defaults())
    engine.index('a', '2', [title: 'two'], IndexOptions.defaults())
    def external = ReindexRequest.of('a', 'a-v2').withVersionType(ReindexRequest.VersionType.EXTERNAL)
        .withProceedOnConflicts(true)

    when:
    def first = engine.reindex(external)

    then:
    first.created() == 2
    first.versionConflicts() == 0
    engine.get('a-v2', '1').get().version() == 2L
    engine.get('a-v2', '2').get().version() == 1L

    when: 'the source changes one document and the destination already has a newer write of another'
    engine.index('a', '2', [title: 'two again'], IndexOptions.defaults())
    engine.index('a-v2', '1', [title: 'written to the new index'], IndexOptions.defaults())
    def second = engine.reindex(external)

    then:
    second.total() == 2
    second.updated() == 1
    second.versionConflicts() == 1
    second.accounted()
    second.ok()
    engine.documents('a-v2') == ['1': [title: 'written to the new index'], '2': [title: 'two again']]
    engine.get('a-v2', '2').get().version() == 2L

    when: 'nothing changed, so every document conflicts'
    def third = engine.reindex(external)

    then:
    third.versionConflicts() == third.total()
    third.accounted()
  }

  def "an external-version reindex that does not proceed on conflicts stops at the first one and reports it"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.createIndex('a-v2', [:], MAPPINGS, [] as Set)
    engine.index('a', '1', [title: 'one'], IndexOptions.defaults())
    engine.index('a', '2', [title: 'two'], IndexOptions.defaults())
    def external = ReindexRequest.of('a', 'a-v2').withVersionType(ReindexRequest.VersionType.EXTERNAL)
    engine.reindex(external)

    when:
    def result = engine.reindex(external)

    then: 'total is still every snapshot document, so the result is not accounted for'
    !result.ok()
    result.versionConflicts() == 1
    result.total() == 2
    !result.accounted()
    !result.complete()
    result.failures().size() == 1
    result.failures()[0].id == '1'
    result.failures()[0].status == 409
    result.failures()[0].cause.type == 'version_conflict_engine_exception'
  }

  def "a delete leaves a tombstone: the version continues on the next write"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.index('a', '1', [title: 'x'], IndexOptions.defaults())
    engine.index('a', '1', [title: 'x'], IndexOptions.defaults())

    when:
    engine.delete('a', '1')

    then:
    engine.tombstoneVersion('a-v1', '1') == 3L
    engine.tombstoneVersion('a-v1', '2') == null

    when:
    def again = engine.index('a', '1', [title: 'y'], IndexOptions.defaults())

    then:
    again.version() == 4L
    again.result() == 'created'
    engine.tombstoneVersion('a-v1', '1') == null

    when: 'a bulk delete leaves one too'
    engine.bulk([BulkOp.delete('a', '1')])

    then:
    engine.tombstoneVersion('a-v1', '1') == 5L
  }

  def "an external-version reindex onto a recently deleted id is a version conflict"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.createIndex('a-v2', [:], MAPPINGS, [] as Set)
    engine.index('a', '1', [title: 'one'], IndexOptions.defaults())
    engine.index('a', '2', [title: 'two'], IndexOptions.defaults())
    def external = ReindexRequest.of('a', 'a-v2').withVersionType(ReindexRequest.VersionType.EXTERNAL)
        .withProceedOnConflicts(true)
    engine.reindex(external)

    when: 'the copy of 1 is deleted from the destination, then the copy runs again'
    engine.delete('a-v2', '1')
    def result = engine.reindex(external)

    then:
    result.total() == 2
    result.versionConflicts() == 2
    result.created() == 0
    result.complete()
    !engine.get('a-v2', '1').present
  }

  def "a conditional write needs an id"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)

    when:
    engine.index('a', null, [title: 'x'], IndexOptions.ifMatch(0, 1))

    then:
    def e = thrown(IllegalArgumentException)
    e.message.contains('needs a document id')
    engine.documents('a-v1').isEmpty()
  }

  def "updateByQuery counts documents changed after it took their sequence numbers as conflicts"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.index('a', '1', [title: 'one'], IndexOptions.defaults())
    engine.index('a', '2', [title: 'two'], IndexOptions.defaults())
    engine.index('a', '3', [title: 'three'], IndexOptions.defaults())
    List<String> seen = []
    engine.duringUpdateByQuery { request ->
      seen << request.index()
      engine.index('a', '1', [title: 'changed'], IndexOptions.defaults())
      engine.delete('a', '3')
    }

    when:
    def result = engine.updateByQuery(UpdateByQueryRequest.of('a').withProceedOnConflicts(true))

    then:
    seen == ['a']
    result.total() == 3
    result.updated() == 1
    result.versionConflicts() == 2
    result.complete()
    engine.get('a', '1').get().version() == 2L
    engine.get('a', '2').get().version() == 2L
  }

  def "updateByQuery without proceedOnConflicts stops at the first changed document with one 409 failure"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.index('a', '1', [title: 'one'], IndexOptions.defaults())
    engine.index('a', '2', [title: 'two'], IndexOptions.defaults())
    engine.duringUpdateByQuery { engine.index('a', '1', [title: 'changed'], IndexOptions.defaults()) }

    when:
    def result = engine.updateByQuery(UpdateByQueryRequest.of('a'))

    then:
    result.total() == 2
    result.updated() == 0
    result.versionConflicts() == 1
    result.failures().size() == 1
    result.failures()[0].id == '1'
    result.failures()[0].status == 409
    result.failures()[0].cause.reason.contains('current document has seqNo [2]')
    !result.ok()
    !result.accounted()
    engine.get('a', '2').get().version() == 1L

    when: 'the hook is removed, nothing conflicts'
    engine.duringUpdateByQuery(null)

    then:
    engine.updateByQuery(UpdateByQueryRequest.of('a')).complete()
  }

  def "putSettings merges flat and nested settings, keeps the getSettings shape and records itself"() {
    given:
    engine.createIndex('a-v1', [number_of_shards: 2], MAPPINGS, ['a'] as Set)

    when:
    engine.putSettings('a', ['index.refresh_interval': '5s', 'index.number_of_replicas': 0])
    engine.putSettings('a-v1', [index: [max_result_window: 500]])

    then:
    engine.getSettings('a') == [index: [number_of_shards: '2', number_of_replicas: '0',
                                        refresh_interval: '5s', max_result_window: '500']]
    engine.log().contains('putSettings a {index.number_of_replicas=0, index.refresh_interval=5s}')

    when: 'a null value resets a setting'
    engine.putSettings('a', ['index.refresh_interval': null])

    then:
    !engine.getSettings('a').index.containsKey('refresh_interval')

    when: 'the write block is a setting too'
    engine.putSettings('a', ['index.blocks.write': true])

    then:
    engine.isWriteBlocked('a-v1')
    engine.getSettings('a').index.blocks == [write: 'true']

    when:
    engine.putSettings('a', [index: [blocks: [write: false, read_only_allow_delete: false]]])

    then:
    !engine.isWriteBlocked('a-v1')
    engine.getSettings('a').index.blocks == [read_only_allow_delete: 'false']

    when:
    engine.putSettings('a', [index: [blocks: [write: true]]])

    then:
    engine.isWriteBlocked('a-v1')

    when:
    engine.putSettings('missing', [:])

    then:
    thrown(EngineException)
  }

  def "afterAliasUpdate runs once the aliases are applied"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.createIndex('a-v2', [:], MAPPINGS, [] as Set)
    List<Set<String>> seen = []
    engine.afterAliasUpdate { seen << engine.getAliases('a').keySet() }

    when:
    engine.updateAliases([AliasAction.remove('a-v1', 'a'), AliasAction.add('a-v2', 'a')])

    then:
    seen == [['a-v2'] as Set]

    when: 'a rejected update does not run the hook'
    engine.updateAliases([AliasAction.remove('a-v1', 'a')])

    then:
    thrown(EngineException)
    seen.size() == 1

    when:
    engine.afterAliasUpdate(null)
    engine.updateAliases([AliasAction.add('a-v1', 'b')])

    then:
    seen.size() == 1
  }

  // ---- aggregations ------------------------------------------------------------------------

  def "compiled terms, metrics, nested and reverse_nested aggregations run over the sample documents"() {
    given:
    def mapper = com.phatjam98.elasticprotos.mapping.ProtoMapper.withDefaults()
    def codec = com.phatjam98.elasticprotos.codec.ProtoDocumentCodec.of(mapper)
    def documents = mapper.map(com.phatjam98.elasticprotos.proto.example.Document)
    def compiler = com.phatjam98.elasticprotos.query.QueryCompiler.withDefaults()
    engine.createIndex(documents.name(), [:], documents.mappings(), [documents.alias()] as Set)
    SampleData.documents().each { engine.index(documents.alias(), it.id, codec.toDocument(it), IndexOptions.defaults()) }

    when:
    def criteria = Criteria.builder().aggregate(
        Aggs.sub(Aggs.terms('by_type', 'type'), Aggs.sum('total', 'size_bytes')),
        Aggs.sub(Aggs.terms('roles', 'parties.role').setTerms(TermsAggregation.newBuilder()
            .addOrder(TermsOrder.newBuilder().setBy(TermsOrderBy.BY_METRIC).setMetric('firms'))),
            Aggs.cardinality('firms', 'firm_id'), Aggs.max('latest', 'filed_at')),
        Aggs.min('smallest', 'size_bytes'), Aggs.max('largest', 'size_bytes'), Aggs.avg('average', 'size_bytes'),
        Aggs.count('counted', 'size_bytes'), Aggs.max('latest', 'filed_at'), Aggs.min('lowest_relevance', 'relevance'),
        Aggs.terms('top_tag', 'tags').setTerms(TermsAggregation.newBuilder().setSize(1)),
        Aggs.terms('by_key', 'type').setTerms(TermsAggregation.newBuilder()
            .addOrder(TermsOrder.newBuilder().setBy(TermsOrderBy.BY_KEY).setDirection(SortingOrder.DESC))),
        Aggs.terms('few', 'firm_id').setTerms(TermsAggregation.newBuilder().setMinDocCount(2)
            .addOrder(TermsOrder.newBuilder().setBy(TermsOrderBy.BY_COUNT).setDirection(SortingOrder.ASC))),
        Aggs.terms('rel', 'relevance').setTerms(TermsAggregation.newBuilder().setMissing(
            com.phatjam98.elasticprotos.proto.PrimitiveValue.newBuilder().setNumberValue(0)))
    ).aggregationsOnly().build()
    def result = engine.search([documents.alias()], compiler.compile(criteria, documents).body())
    def aggs = AggregationResults.of(result)

    then:
    result.hits().isEmpty()
    result.total() == 6
    aggs.buckets('by_type')*.key() == ['FILING', 'MOTION', 'NOTICE', 'ORDER']
    aggs.buckets('by_type')*.docCount() == [3L, 1L, 1L, 1L]
    aggs.buckets('by_type')*.sub()*.metric('total')*.asLong() == [2_870_000L, 45_000L, 8_000L, 30_000L]
    aggs.buckets('roles')*.key() == ['DEFENDANT', 'PLAINTIFF', 'COUNSEL']
    aggs.buckets('roles')*.docCount() == [4L, 3L, 1L]
    aggs.buckets('roles')*.sub()*.metric('firms')*.asLong() == [3L, 2L, 1L]
    aggs.buckets('roles')[0].sub().metric('latest').valueAsString() == '2024-06-30T08:00:00.000Z'
    aggs.metric('smallest').asLong() == 8_000L
    aggs.metric('largest').asLong() == 2_500_000L
    Math.abs(aggs.metric('average').value() - 2_953_000d / 6) < 0.01
    aggs.metric('counted').asLong() == 6L
    aggs.metric('latest').asLong() == 1719734400000L
    aggs.metric('lowest_relevance').value() == 0.75d
    aggs.buckets('top_tag')*.key() == ['civil']
    aggs.sumOtherDocCount('top_tag') == 6
    aggs.buckets('by_key')*.key() == ['ORDER', 'NOTICE', 'MOTION', 'FILING']
    aggs.buckets('few')*.key() == ['firm-1', 'firm-2', 'firm-3']
    aggs.buckets('rel').collectEntries { [(it.key()): it.docCount()] } == [0L: 5L, 0.75d: 1L]
  }

  def "terms keys follow the engines: booleans 1/0, dates epoch millis, .keyword reads the text"() {
    given:
    engine.createIndex('t-v1', [:], [properties: [
        flag : [type: 'boolean'],
        at   : [type: 'date'],
        title: [type: 'text', fields: [keyword: [type: 'keyword']]],
        owner: [type: 'object', properties: [name: [type: 'keyword']]]]], ['t'] as Set)
    engine.index('t', '1', [flag: true, at: '2024-06-30T08:00:00Z', title: 'Alpha', owner: [name: 'x']], IndexOptions.defaults())
    engine.index('t', '2', [flag: false, at: '2024-01-15T10:30:00.250Z', title: 'Beta', owner: [name: 'x']], IndexOptions.defaults())
    engine.index('t', '3', [flag: true, at: 1704067200000L, title: 'Alpha', owner: [name: 'y']], IndexOptions.defaults())

    when:
    def aggs = AggregationResults.of(engine.search(['t'], [size: 0, aggs: [
        flags : [terms: [field: 'flag']],
        days  : [terms: [field: 'at', order: [_key: 'asc']]],
        titles: [terms: [field: 'title.keyword']],
        owners: [terms: [field: 'owner.name', order: [[_count: 'asc'], [_key: 'desc']]]],
        latest: [max: [field: 'at']]]]))

    then:
    aggs.buckets('flags')*.key() == [1L, 0L]
    aggs.buckets('flags')*.keyAsString() == ['true', 'false']
    aggs.buckets('flags')*.docCount() == [2L, 1L]
    aggs.buckets('days')*.key() == [1704067200000L, 1705314600250L, 1719734400000L]
    aggs.buckets('days')*.keyAsString() == ['2024-01-01T00:00:00.000Z', '2024-01-15T10:30:00.250Z', '2024-06-30T08:00:00.000Z']
    aggs.buckets('titles')*.key() == ['Alpha', 'Beta']
    aggs.buckets('titles')*.keyAsString() == [null, null]
    aggs.buckets('owners')*.key() == ['y', 'x']
    aggs.metric('latest').valueAsString() == '2024-06-30T08:00:00.000Z'

    when:
    engine.search(['t'], [aggs: [x: [terms: [field: 'at']]]])
    engine.index('t', '4', [at: 'not a date'], IndexOptions.defaults())
    engine.search(['t'], [aggs: [x: [terms: [field: 'at']]]])

    then:
    def e = thrown(EngineException)
    e.message.contains('failed to parse date field')
  }

  def "a metric of nothing is null, and unsupported aggregations fail with BAD_REQUEST"() {
    given:
    engine.createIndex('a-v1', [:], MAPPINGS, ['a'] as Set)
    engine.index('a', '1', [status: 'x', title: 'Not a date', owner: [name: 'n']], IndexOptions.defaults())

    expect:
    engine.search(['a'], [aggs: [m: [min: [field: 'missing']], s: [sum: [field: 'missing']]]]).aggregations() ==
        [m: [value: null], s: [value: 0d]]
    engine.search(['a'], [aggregations: [c: [cardinality: [field: 'status']]]]).aggregations() == [c: [value: 1L]]

    when:
    engine.search(['a'], [aggs: [x: body]])

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.BAD_REQUEST
    e.message.contains(message)

    where:
    body                                                              || message
    [date_histogram: [field: 'd', calendar_interval: '1M']]           || 'not [date_histogram]'
    [histogram: [field: 'n', interval: 1]]                            || 'not [histogram]'
    [range: [field: 'n', ranges: [[to: 1]]]]                          || 'not [range]'
    [date_range: [field: 'n', ranges: [[to: 1]]]]                     || 'not [date_range]'
    [meta: [a: 1]]                                                    || 'has no type'
    [max: [field: 'title']]                                           || 'is not supported for aggregation [max]'
    [max: [field: 'status'], aggs: [y: [min: [field: 'status']]]]    || 'cannot have sub-aggregations'
    [terms: [field: 'status', order: [_key: 'asc', _count: 'desc']]] || 'exactly one key'
    [reverse_nested: [path: 'owner']]                                 || 'does not enclose'
    [nested: [path: 'owner'], aggs: [y: [terms: [field: 'status']]]] || 'is not inside nested path [owner]'
  }
}
