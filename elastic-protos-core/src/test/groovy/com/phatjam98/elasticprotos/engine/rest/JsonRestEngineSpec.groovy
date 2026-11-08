package com.phatjam98.elasticprotos.engine.rest

import com.phatjam98.elasticprotos.engine.AliasAction
import com.phatjam98.elasticprotos.engine.BulkOp
import com.phatjam98.elasticprotos.engine.EngineException
import com.phatjam98.elasticprotos.engine.EngineProduct
import com.phatjam98.elasticprotos.engine.IndexOptions
import com.phatjam98.elasticprotos.engine.Pit
import com.phatjam98.elasticprotos.engine.ReindexRequest
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Duration

class JsonRestEngineSpec extends Specification {

  static final Map ES_ROOT = [cluster_name: 'es', version: [number: '9.1.2', build_flavor: 'default'], tagline: 'You Know, for Search']
  static final Map OS_ROOT = [cluster_name: 'os', version: [number: '2.19.0', distribution: 'opensearch']]

  FakeHttp http = new FakeHttp()
  JsonRestEngine engine = new JsonRestEngine(http, Duration.ofMillis(1))

  def "info detects the product and caches it"() {
    given:
    http.enqueue(200, ES_ROOT)

    when:
    def info = engine.info()
    engine.info()

    then:
    info.product() == EngineProduct.ELASTICSEARCH
    info.version() == '9.1.2'
    info.clusterName() == 'es'
    info.supportsPointInTime()
    info.major() == 9
    http.requests.size() == 1
    http.last.method == 'GET'
    http.last.path == '/'
  }

  def "info recognises OpenSearch and unknown servers"() {
    expect:
    new JsonRestEngine(new FakeHttp().enqueue(200, OS_ROOT)).info().product() == EngineProduct.OPENSEARCH
    new JsonRestEngine(new FakeHttp().enqueue(200, [hello: 'world'])).info().product() == EngineProduct.UNKNOWN
    !new JsonRestEngine(new FakeHttp().enqueue(200, [hello: 'world'])).info().supportsPointInTime()
    !new JsonRestEngine(new FakeHttp().enqueue(200, [version: [number: '2.3.0', distribution: 'opensearch']])).info().supportsShardDoc()
    !new JsonRestEngine(new FakeHttp().enqueue(200, [version: [number: '3.2.0', distribution: 'opensearch']])).info().supportsShardDoc()
    new JsonRestEngine(new FakeHttp().enqueue(200, [version: [number: '3.3.0', distribution: 'opensearch']])).info().supportsShardDoc()
    !new JsonRestEngine(new FakeHttp().enqueue(200, [version: [number: '7.11.2']])).info().supportsShardDoc()
    new JsonRestEngine(new FakeHttp().enqueue(200, [version: [number: '7.12.0']])).info().supportsShardDoc()
    !new JsonRestEngine(new FakeHttp().enqueue(200, [hello: 'world'])).info().supportsShardDoc()
    new JsonRestEngine(new FakeHttp().enqueue(200, [version: [number: 'weird']])).info().minor() == 0
  }

  def "indexExists uses HEAD"() {
    given:
    http.enqueue(200).enqueue(404)

    expect:
    engine.indexExists('docs')
    !engine.indexExists('nope')
    http.requests*.method == ['HEAD', 'HEAD']
    http.requests*.path == ['/docs', '/nope']
  }

  def "createIndex sends settings, mappings and aliases in one request"() {
    given:
    http.enqueue(200, [acknowledged: true])

    when:
    engine.createIndex('docs-v1', [number_of_shards: 1], [dynamic: 'strict', properties: [:]], ['docs'] as Set)

    then:
    http.last.method == 'PUT'
    http.last.path == '/docs-v1'
    http.last.json() == [aliases: [docs: [:]], mappings: [dynamic: 'strict', properties: [:]], settings: [number_of_shards: 1]]
    http.last.contentType == RawHttp.JSON
  }

  def "createIndex omits empty settings and aliases"() {
    given:
    http.enqueue(200)

    when:
    engine.createIndex('docs-v1', [:], [properties: [:]], [] as Set)

    then:
    http.last.json() == [mappings: [properties: [:]]]
  }

  def "deleteIndex, putMappings, setWriteBlock and refresh hit the expected endpoints"() {
    given:
    http.enqueue(200).enqueue(200).enqueue(200).enqueue(200)

    when:
    engine.deleteIndex('old')
    engine.putMappings('docs-v1', [properties: [a: [type: 'keyword']]])
    engine.setWriteBlock('old', true)
    engine.refresh('docs')

    then:
    http.requests*.method == ['DELETE', 'PUT', 'PUT', 'POST']
    http.requests*.path == ['/old', '/docs-v1/_mapping', '/old/_settings', '/docs/_refresh']
    http.requests[1].json() == [properties: [a: [type: 'keyword']]]
    http.requests[2].json() == ['index.blocks.write': true]
  }

  def "flattened fields are spelled flat_object on OpenSearch and read back as flattened"() {
    given:
    def mappings = [_meta: [type: 'flattened'], properties: [meta: [type: 'flattened'], doc: [properties: [inner: [type: 'flattened'], type: [type: 'keyword']]]]]
    http.enqueue(200, OS_ROOT).enqueue(200).enqueue(200)
        .enqueue(200, ['docs-v1': [mappings: [properties: [meta: [type: 'flat_object'], doc: [properties: [inner: [type: 'flat_object']]]]]]])

    when:
    engine.createIndex('docs-v1', [:], mappings, [] as Set)
    engine.putMappings('docs-v1', mappings)
    def read = engine.getMappings('docs-v1')

    then:
    http.requests*.path == ['/', '/docs-v1', '/docs-v1/_mapping', '/docs-v1/_mapping']
    http.requests[1].json().mappings == [_meta: [type: 'flattened'], properties: [meta: [type: 'flat_object'], doc: [properties: [inner: [type: 'flat_object'], type: [type: 'keyword']]]]]
    http.requests[2].json().properties.meta == [type: 'flat_object']
    read == [properties: [meta: [type: 'flattened'], doc: [properties: [inner: [type: 'flattened']]]]]
  }

  def "flattened fields are sent unchanged to Elasticsearch, and mappings without them skip the lookup"() {
    given:
    http.enqueue(200, ES_ROOT).enqueue(200).enqueue(200)

    when:
    engine.createIndex('a', [:], [properties: [m: [type: 'flattened']]], [] as Set)
    engine.createIndex('b', [:], [properties: [k: [type: 'keyword']]], [] as Set)

    then:
    http.requests*.path == ['/', '/a', '/b']
    http.requests[1].json().mappings == [properties: [m: [type: 'flattened']]]
  }

  def "getMappings and getSettings unwrap the single index behind an alias"() {
    given:
    http.enqueue(200, ['docs-v1': [mappings: [properties: [a: [type: 'keyword']]]]])
        .enqueue(200, ['docs-v1': [settings: [index: [number_of_shards: '1']]]])

    expect:
    engine.getMappings('docs') == [properties: [a: [type: 'keyword']]]
    engine.getSettings('docs') == [index: [number_of_shards: '1']]
    http.requests*.path == ['/docs/_mapping', '/docs/_settings']
  }

  def "getMappings refuses an alias that spans several indices and reports a missing one"() {
    given:
    http.enqueue(200, [a: [mappings: [:]], b: [mappings: [:]]])
        .enqueue(404, [error: [type: 'index_not_found_exception', reason: 'no such index [x]'], status: 404])

    when:
    engine.getMappings('docs')

    then:
    def conflict = thrown(EngineException)
    conflict.kind == EngineException.Kind.CONFLICT

    when:
    engine.getMappings('x')

    then:
    def missing = thrown(EngineException)
    missing.kind == EngineException.Kind.NOT_FOUND
    missing.status == 404
    missing.message == 'index_not_found_exception: no such index [x]'
  }

  def "getAliases resolves aliases and indices, and 404 means nothing"() {
    given:
    http.enqueue(200, ['docs-v1': [aliases: [docs: [:], all: [:]]], 'docs-v2': [aliases: [:]]]).enqueue(404)

    expect:
    engine.getAliases('docs') == ['docs-v1': ['all', 'docs'] as Set, 'docs-v2': [] as Set]
    engine.getAliases('missing') == [:]
    http.requests*.path == ['/docs/_alias', '/missing/_alias']
  }

  def "updateAliases posts every action atomically"() {
    given:
    http.enqueue(200, [acknowledged: true])

    when:
    engine.updateAliases([AliasAction.remove('docs-v1', 'docs'), AliasAction.add('docs-v2', 'docs'), AliasAction.removeIndex('tmp'), AliasAction.remove(['a', 'b'], 'x')])

    then:
    http.last.path == '/_aliases'
    http.last.json() == [actions: [
        [remove: [alias: 'docs', indices: ['docs-v1']]],
        [add: [alias: 'docs', indices: ['docs-v2']]],
        [remove_index: [indices: ['tmp']]],
        [remove: [alias: 'x', indices: ['a', 'b']]],
    ]]
  }

  def "reindex starts a task, polls it and reports the outcome"() {
    given:
    http.enqueue(200, [task: 'node:1'])
        .enqueue(200, [completed: false])
        .enqueue(200, [completed: true, response: [total: 3, created: 2, updated: 1, failures: [], took: 42, timed_out: false]])

    when:
    def result = engine.reindex(ReindexRequest.of('docs', 'docs-v2').withScript('ctx._source.x = params.v', [v: 1]))

    then:
    result.ok()
    result.total() == 3
    result.created() == 2
    result.updated() == 1
    result.tookMs() == 42
    http.requests*.path == ['/_reindex', '/_tasks/node:1', '/_tasks/node:1']
    http.requests[0].params == [refresh: 'true', wait_for_completion: 'false']
    http.requests[0].json() == [dest: [index: 'docs-v2'], script: [lang: 'painless', params: [v: 1], source: 'ctx._source.x = params.v'], source: [index: 'docs']]
  }

  def "reindex surfaces failures, task errors and timeouts"() {
    given:
    def failing = new FakeHttp().enqueue(200, [task: 't']).enqueue(200, [completed: true, response: [total: 1, failures: [[id: '1', cause: [reason: 'boom']]]]])
    def errored = new FakeHttp().enqueue(200, [task: 't']).enqueue(200, [completed: true, error: [reason: 'script_exception']])
    def slow = new FakeHttp().enqueue(200, [task: 't']).enqueue(200, [completed: false]).enqueue(200, [completed: false]).enqueue(200, [completed: false])

    expect:
    !new JsonRestEngine(failing, Duration.ofMillis(1)).reindex(ReindexRequest.of('a', 'b')).ok()
    new JsonRestEngine(failing, Duration.ofMillis(1)).reindexPollInterval() == Duration.ofMillis(1)

    when:
    new JsonRestEngine(errored, Duration.ofMillis(1)).reindex(ReindexRequest.of('a', 'b'))

    then:
    def errorEx = thrown(EngineException)
    errorEx.message.contains('script_exception')

    when:
    new JsonRestEngine(slow, Duration.ofMillis(1)).reindex(ReindexRequest.of('a', 'b').withTimeout(Duration.ofMillis(2)))

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.TIMEOUT
  }

  def "index, create, update and delete documents"() {
    given:
    http.enqueue(201, [_index: 'docs-v1', _id: '1', result: 'created', _version: 1])
        .enqueue(201, [_index: 'docs-v1', _id: 'gen', result: 'created', _version: 1])
        .enqueue(201, [_index: 'docs-v1', _id: '2', result: 'created', _version: 1])
        .enqueue(200, [_index: 'docs-v1', _id: '1', result: 'updated', _version: 2])
        .enqueue(200, [_index: 'docs-v1', _id: '1', result: 'deleted', _version: 3])
        .enqueue(404, [_index: 'docs-v1', _id: '9', result: 'not_found'])

    when:
    def created = engine.index('docs', '1', [a: 1], IndexOptions.refreshed())
    def generated = engine.index('docs', null, [a: 1], IndexOptions.defaults())
    engine.index('docs', '2', [a: 1], new IndexOptions(true, IndexOptions.Refresh.WAIT_FOR))
    def updated = engine.update('docs', '1', [a: 2])

    then:
    created.id() == '1' && created.result() == 'created' && created.version() == 1
    generated.id() == 'gen'
    updated.result() == 'updated'
    http.requests[0].method == 'PUT' && http.requests[0].path == '/docs/_doc/1' && http.requests[0].params == [refresh: 'true']
    http.requests[1].method == 'POST' && http.requests[1].path == '/docs/_doc' && http.requests[1].params == [:]
    http.requests[2].path == '/docs/_create/2' && http.requests[2].params == [refresh: 'wait_for']
    http.requests[3].method == 'POST' && http.requests[3].path == '/docs/_update/1' && http.requests[3].json() == [doc: [a: 2]]

    and:
    engine.delete('docs', '1')
    !engine.delete('docs', '9')
    http.requests[4].method == 'DELETE' && http.requests[4].path == '/docs/_doc/1' && http.requests[4].params == [:]
  }

  def "document ids are percent-encoded as a single path segment"() {
    given:
    String id = 'a/b c?d#e%'
    String encoded = 'a%2Fb%20c%3Fd%23e%25'
    5.times { http.enqueue(200, [_index: 'idx', _id: id, result: 'created', _version: 1, found: true, _source: [:]]) }

    when:
    engine.index('idx', id, [a: 1], IndexOptions.defaults())
    engine.index('idx', id, [a: 1], new IndexOptions(true, IndexOptions.Refresh.NONE))
    engine.update('idx', id, [a: 2])
    engine.get('idx', id)
    engine.delete('idx', id)

    then:
    http.requests*.path == [
        "/idx/_doc/${encoded}", "/idx/_create/${encoded}", "/idx/_update/${encoded}",
        "/idx/_doc/${encoded}", "/idx/_doc/${encoded}"]
  }

  def "non-ASCII ids are UTF-8 percent-encoded and index names are left alone"() {
    given:
    http.enqueue(200, [_index: 'a', _id: 'é', result: 'created', _version: 1])

    when:
    engine.index('a,b-*', 'é', [a: 1], IndexOptions.defaults())

    then:
    http.last.path == '/a,b-*/_doc/%C3%A9'
  }

  def "delete passes the refresh policy"() {
    given:
    http.enqueue(200, [_index: 'docs-v1', _id: '1', result: 'deleted', _version: 2])
        .enqueue(200, [_index: 'docs-v1', _id: '2', result: 'deleted', _version: 2])

    expect:
    engine.delete('docs', '1', IndexOptions.refreshed())
    engine.delete('docs', '2', new IndexOptions(false, IndexOptions.Refresh.WAIT_FOR))
    http.requests[0].params == [refresh: 'true']
    http.requests[1].params == [refresh: 'wait_for']
  }

  def "get reads a document through the realtime document API"() {
    given:
    http.enqueue(200, [_index: 'docs-v1', _id: '1', _version: 3, _seq_no: 4, _primary_term: 1, found: true, _source: [a: 1]])

    when:
    def hit = engine.get('docs', '1')

    then:
    http.last.method == 'GET'
    http.last.path == '/docs/_doc/1'
    http.last.params == [:]
    http.last.body == null
    hit.present
    hit.get().index() == 'docs-v1'
    hit.get().id() == '1'
    hit.get().score() == null
    hit.get().sort() == []
    hit.get().source() == [a: 1]
  }

  def "get is empty for a missing document or index: #body"() {
    given:
    http.enqueue(status, body)

    expect:
    !engine.get('docs', '9').present

    where:
    status | body
    404    | [_index: 'docs-v1', _id: '9', found: false]
    404    | [error: [type: 'index_not_found_exception', reason: 'no such index [docs]'], status: 404]
    200    | [_index: 'docs-v1', _id: '9', found: false]
  }

  def "get reports other failures"() {
    given:
    http.enqueue(500, [error: [type: 'null_pointer_exception', reason: 'boom'], status: 500])

    when:
    engine.get('docs', '1')

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.OTHER
    e.status == 500
    e.message == 'null_pointer_exception: boom'
  }

  def "bulk writes NDJSON and parses every item"() {
    given:
    http.enqueue(200, [took: 7, errors: true, items: [
        [index: [_index: 'docs-v1', _id: '1', status: 201]],
        [create: [_index: 'docs-v1', _id: '2', status: 409, error: [type: 'version_conflict_engine_exception', reason: 'exists']]],
        [update: [_index: 'docs-v1', _id: '3', status: 200]],
        [delete: [_index: 'docs-v1', _id: '4', status: 200]],
    ]])

    when:
    def result = engine.bulk([
        BulkOp.index('docs', '1', [a: 1]),
        BulkOp.create('docs', '2', [a: 2]),
        BulkOp.update('docs', '3', [a: 3]),
        BulkOp.delete('docs', '4'),
        BulkOp.index('docs', null, [a: 5]),
    ])

    then:
    http.last.path == '/_bulk'
    http.last.contentType == RawHttp.NDJSON
    http.last.body == '{"index":{"_id":"1","_index":"docs"}}\n{"a":1}\n' +
        '{"create":{"_id":"2","_index":"docs"}}\n{"a":2}\n' +
        '{"update":{"_id":"3","_index":"docs"}}\n{"doc":{"a":3}}\n' +
        '{"delete":{"_id":"4","_index":"docs"}}\n' +
        '{"index":{"_index":"docs"}}\n{"a":5}\n'
    result.errors()
    result.tookMs() == 7
    result.items().size() == 4
    result.items()[0].ok() && result.items()[0].op() == 'index' && result.items()[0].status() == 201
    result.failures()*.id() == ['2']
    result.failures()[0].error() == 'version_conflict_engine_exception: exists'
  }

  def "search parses hits, totals, sort values, aggregations and the pit id"() {
    given:
    http.enqueue(200, [took: 3, pit_id: 'p2', hits: [total: [value: 10000, relation: 'gte'], hits: [
        [_index: 'docs-v1', _id: '1', _score: 1.5, _source: [a: 1], sort: [1, 'x']],
        [_index: 'docs-v1', _id: '2', _score: null, _source: [a: 2]],
    ]], aggregations: [by_type: [buckets: []]]])

    when:
    def result = engine.search(['docs'], [query: [match_all: [:]], pit: [id: 'p1', keep_alive: '1m']])

    then:
    http.last.path == '/_search'
    result.total() == 10000
    !result.totalExact()
    result.hits().size() == 2
    result.hits()[0].score() == 1.5d
    result.hits()[0].sort() == [1, 'x']
    result.hits()[1].score() == null
    result.hits()[1].sort() == []
    result.lastSort() == []
    result.pitId() == 'p2'
    result.aggregations() == [by_type: [buckets: []]]
    result.tookMs() == 3
  }

  def "search without a pit targets the indices"() {
    given:
    http.enqueue(200, [hits: [total: [value: 0, relation: 'eq'], hits: []]])

    when:
    def result = engine.search(['a', 'b'], [query: [match_all: [:]]])

    then:
    http.last.path == '/a,b/_search'
    result.total() == 0
    result.totalExact()
    result.lastSort() == null
    result.hits().isEmpty()
  }

  def "count searches with size 0 and exact totals"() {
    given:
    http.enqueue(200, [hits: [total: [value: 42, relation: 'eq'], hits: []]])

    when:
    long count = engine.count('docs')

    then:
    count == 42
    http.last.method == 'POST'
    http.last.path == '/docs/_search'
    http.last.json() == [size: 0, track_total_hits: true]
  }

  @Unroll
  def "point in time endpoints differ per product: #product"() {
    given:
    http.enqueue(200, root).enqueue(200, [(idKey): 'pit-1']).enqueue(200, [succeeded: true])

    when:
    Pit pit = engine.openPit(['docs'], Duration.ofMinutes(2))
    engine.closePit(pit)

    then:
    pit.id() == 'pit-1'
    pit.keepAlive() == Duration.ofMinutes(2)
    http.requests[1].method == 'POST'
    http.requests[1].path == openPath
    http.requests[1].params == [keep_alive: '2m']
    http.requests[2].method == 'DELETE'
    http.requests[2].path == closePath
    http.requests[2].json() == closeBody

    where:
    product        | root    | idKey    | openPath                    | closePath                | closeBody
    'ELASTICSEARCH'| ES_ROOT | 'id'     | '/docs/_pit'                | '/_pit'                  | [id: 'pit-1']
    'OPENSEARCH'   | OS_ROOT | 'pit_id' | '/docs/_search/point_in_time' | '/_search/point_in_time' | [pit_id: ['pit-1']]
  }

  def "health is summarised"() {
    given:
    http.enqueue(200, [status: 'yellow', number_of_nodes: 3, number_of_data_nodes: 2])

    expect:
    with(engine.health()) {
      status() == 'yellow'
      numberOfNodes() == 3
      numberOfDataNodes() == 2
    }
  }

  @Unroll
  def "errors are classified: #status #type -> #kind"() {
    given:
    http.enqueue(status, type == null ? 'plain text failure' : [error: [type: type, reason: 'r']])

    when:
    engine.deleteIndex('x')

    then:
    def e = thrown(EngineException)
    e.kind == kind
    e.status == status
    e.message == (type == null ? 'plain text failure' : "${type}: r")

    where:
    status | type                                | kind
    400    | 'resource_already_exists_exception' | EngineException.Kind.ALREADY_EXISTS
    400    | 'illegal_argument_exception'        | EngineException.Kind.BAD_REQUEST
    401    | 'security_exception'                | EngineException.Kind.UNAUTHORIZED
    403    | 'security_exception'                | EngineException.Kind.UNAUTHORIZED
    404    | 'index_not_found_exception'         | EngineException.Kind.NOT_FOUND
    404    | null                                | EngineException.Kind.NOT_FOUND
    409    | 'version_conflict_engine_exception' | EngineException.Kind.CONFLICT
    408    | null                                | EngineException.Kind.TIMEOUT
    429    | 'es_rejected_execution_exception'   | EngineException.Kind.TRANSIENT
    503    | null                                | EngineException.Kind.TRANSIENT
    500    | 'null_pointer_exception'            | EngineException.Kind.OTHER
  }

  def "transport failures become IO errors and close is forwarded"() {
    given:
    http.failure = new IOException('connection refused')

    when:
    engine.indexExists('x')

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.IO
    e.cause instanceof IOException
    e.message == 'HEAD /x failed: connection refused'

    when:
    engine.close()

    then:
    http.closed
  }

  def "transport failures name the endpoints the transport describes"() {
    given:
    def described = new FakeHttp() {
      @Override
      String describe() { 'http://localhost:9200, http://other:9200' }
    }
    described.failure = new IOException('No route to host')

    when:
    new JsonRestEngine(described).getAliases('documents')

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.IO
    e.message == 'GET /documents/_alias on http://localhost:9200, http://other:9200 failed: No route to host'
  }

  def "a transport that describes itself as null is left out of messages"() {
    given:
    def anonymous = new FakeHttp() {
      @Override
      String describe() { null }
    }
    anonymous.failure = new IOException('reset')

    when:
    new JsonRestEngine(anonymous).refresh('x')

    then:
    def e = thrown(EngineException)
    e.message == 'POST /x/_refresh failed: reset'
  }

  def "a transport that fails to close is reported"() {
    given:
    def broken = new FakeHttp() {
      @Override
      void close() { throw new IOException('stuck') }
    }

    when:
    new JsonRestEngine(broken).close()

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.IO
  }

  @Unroll
  def "durations are formatted as engine time values: #duration -> #expected"() {
    expect:
    JsonRestEngine.timeValue(duration) == expected

    where:
    duration                 | expected
    Duration.ofMinutes(5)    | '5m'
    Duration.ofSeconds(90)   | '90s'
    Duration.ofMillis(1500)  | '1500ms'
    Duration.ofHours(1)      | '60m'
  }
}
