package com.phatjam98.elasticprotos.testkit

import com.phatjam98.elasticprotos.engine.AliasAction
import com.phatjam98.elasticprotos.engine.BulkOp
import com.phatjam98.elasticprotos.engine.EngineException
import com.phatjam98.elasticprotos.engine.EngineInfo
import com.phatjam98.elasticprotos.engine.EngineProduct
import com.phatjam98.elasticprotos.engine.IndexOptions
import com.phatjam98.elasticprotos.engine.ReindexRequest
import com.phatjam98.elasticprotos.engine.SearchResult
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
}
