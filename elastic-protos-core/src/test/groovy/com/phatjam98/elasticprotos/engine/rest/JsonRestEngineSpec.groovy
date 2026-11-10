package com.phatjam98.elasticprotos.engine.rest

import com.phatjam98.elasticprotos.engine.AliasAction
import com.phatjam98.elasticprotos.engine.BulkOp
import com.phatjam98.elasticprotos.engine.EngineException
import com.phatjam98.elasticprotos.engine.EngineProduct
import com.phatjam98.elasticprotos.engine.IndexOptions
import com.phatjam98.elasticprotos.engine.Pit
import com.phatjam98.elasticprotos.engine.ReindexRequest
import com.phatjam98.elasticprotos.engine.SearchResult
import com.phatjam98.elasticprotos.engine.UpdateByQueryRequest
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
    http.requests*.path == ['/old', '/docs-v1/_mapping', '/old/_block/write', '/docs/_refresh']
    http.requests[1].json() == [properties: [a: [type: 'keyword']]]
    http.requests[2].body == null
  }

  def "seqNoCheckpoint is the lowest max_seq_no over every shard copy"() {
    given:
    http.enqueue(200, [indices: [docs: [shards: [
        '0': [[routing: [primary: true], seq_no: [max_seq_no: 7]], [routing: [primary: false], seq_no: [max_seq_no: 5]]],
        '1': [[routing: [primary: true], seq_no: [max_seq_no: 9]]]]]]])
    http.enqueue(200, [indices: [:]])

    expect:
    engine.seqNoCheckpoint('docs') == 5
    http.requests[0].method == 'GET'
    http.requests[0].path == '/docs/_stats'
    http.requests[0].params == [level: 'shards']
    engine.seqNoCheckpoint('docs') == -1
  }

  def "globalCheckpoint is the lowest primary global_checkpoint, once every primary reported"() {
    given:
    def copy = { boolean primary, Object gcp -> [routing: [primary: primary], seq_no: [max_seq_no: 99, global_checkpoint: gcp]] }
    http.enqueue(200, [indices: [docs: [shards: ['0': [copy(true, 7), copy(false, 2)], '1': [copy(true, 4)]]]]])
        .enqueue(200, [docs: [settings: [index: [number_of_shards: '2']]]])
    http.enqueue(200, [indices: [docs: [shards: ['0': [copy(true, -1)]]]]])
        .enqueue(200, [docs: [settings: [index: [number_of_shards: '1']]]])
    http.enqueue(200, [indices: [docs: [shards: ['0': [copy(true, 7)]]]]])
        .enqueue(200, [docs: [settings: [index: [number_of_shards: '2']]]])
    http.enqueue(200, [indices: [docs: [shards: ['0': [copy(true, null)]]]]])
        .enqueue(200, [docs: [settings: [index: [number_of_shards: '1']]]])
    http.enqueue(200, [indices: [docs: [shards: ['0': [copy(false, 3)]]]]])
        .enqueue(200, [docs: [settings: [index: [number_of_shards: '1']]]])
    http.enqueue(200, [indices: [docs: [shards: ['0': [copy(true, 3)]]]]])
        .enqueue(200, [docs: [settings: [index: [number_of_shards: 'many']]]])
    http.enqueue(200, [indices: [docs: [shards: ['0': [copy(true, 3)]]]]])
        .enqueue(200, [docs: [settings: [index: [:]]]])

    expect: 'replicas are ignored; an empty shard is -1'
    engine.globalCheckpoint('docs') == OptionalLong.of(4)
    http.requests[0].path == '/docs/_stats'
    http.requests[0].params == [level: 'shards']
    engine.globalCheckpoint('docs') == OptionalLong.of(-1)

    and: 'a missing primary, a missing value, no primary or an unreadable shard count is unknown'
    engine.globalCheckpoint('docs') == OptionalLong.empty()
    engine.globalCheckpoint('docs') == OptionalLong.empty()
    engine.globalCheckpoint('docs') == OptionalLong.empty()
    engine.globalCheckpoint('docs') == OptionalLong.empty()
    engine.globalCheckpoint('docs') == OptionalLong.empty()
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

  def "flattened values of typed map templates are spelled flat_object on OpenSearch"() {
    given:
    def templates = [['ep:s.*': [path_match: 's.*', mapping: [type: 'flattened']]],
                     ['ep:k.*': [path_match: 'k.*', mapping: [type: 'keyword']]], 'other']
    http.enqueue(200, OS_ROOT).enqueue(200)
        .enqueue(200, ['docs-v1': [mappings: [dynamic_templates: [['ep:s.*': [path_match: 's.*', mapping: [type: 'flat_object']]]]]]])

    when:
    engine.createIndex('docs-v1', [:], [dynamic_templates: templates, properties: [:]], [] as Set)
    def read = engine.getMappings('docs-v1')

    then:
    http.requests[1].json().mappings.dynamic_templates == [
        ['ep:s.*': [path_match: 's.*', mapping: [type: 'flat_object']]],
        ['ep:k.*': [path_match: 'k.*', mapping: [type: 'keyword']]], 'other']
    read.dynamic_templates == [['ep:s.*': [path_match: 's.*', mapping: [type: 'flattened']]]]
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

  def "reindex sends conflicts, the source query and the external version type, and reads every counter"() {
    given:
    http.enqueue(200, [task: 'node:2'])
        .enqueue(200, [completed: true, response: [total: 9, created: 1, updated: 2, noops: 1, deleted: 2,
            version_conflicts: 3, failures: [], took: 5, timed_out: false]])

    when:
    def result = engine.reindex(ReindexRequest.builder().source('docs').dest('docs-v2')
        .versionType(ReindexRequest.VersionType.EXTERNAL).proceedOnConflicts(true)
        .query([ids: [values: ['1', '2']]]).refresh(false).build())

    then:
    http.requests[0].params == [refresh: 'false', wait_for_completion: 'false']
    http.requests[0].body == '{"conflicts":"proceed","dest":{"index":"docs-v2","version_type":"external"},' +
        '"source":{"index":"docs","query":{"ids":{"values":["1","2"]}}}}'
    result.total() == 9
    result.created() == 1
    result.updated() == 2
    result.noops() == 1
    result.deleted() == 2
    result.versionConflicts() == 3
    result.accounted()
    result.ok()
  }

  def "a default reindex leaves out conflicts, the query and the version type"() {
    given:
    http.enqueue(200, [task: 't']).enqueue(200, [completed: true, response: [total: 0]])

    when:
    def result = engine.reindex(ReindexRequest.of('a', 'b'))

    then:
    http.requests[0].body == '{"dest":{"index":"b"},"source":{"index":"a"}}'
    result.versionConflicts() == 0
    result.accounted()
  }

  def "updateByQuery starts a task on the index, polls it and reports the outcome"() {
    given:
    http.enqueue(200, [task: 'node:3'])
        .enqueue(200, [completed: false])
        .enqueue(200, [completed: true, response: [total: 4, updated: 3, version_conflicts: 1, noops: 0, failures: [], took: 9]])

    when:
    def result = engine.updateByQuery(UpdateByQueryRequest.builder().index('docs')
        .query([ids: [values: ['1']]]).proceedOnConflicts(true).build())

    then:
    http.requests.collect { "${it.method} ${it.path}".toString() } ==
        ['POST /docs/_update_by_query', 'GET /_tasks/node:3', 'GET /_tasks/node:3']
    http.requests[0].params == [conflicts: 'proceed', refresh: 'true', wait_for_completion: 'false']
    http.requests[0].json() == [query: [ids: [values: ['1']]]]
    result.total() == 4
    result.updated() == 3
    result.versionConflicts() == 1
    result.tookMs() == 9
    result.accounted()
    result.ok()
  }

  def "updateByQuery without proceedOnConflicts aborts on conflicts and matches everything by default"() {
    given:
    http.enqueue(200, [task: 'n:1']).enqueue(200, [completed: true, response: [total: 0]])

    when:
    engine.updateByQuery(UpdateByQueryRequest.of('docs').withRefresh(false))

    then:
    http.requests[0].params == [refresh: 'false', wait_for_completion: 'false']
    http.requests[0].json() == [query: [match_all: [:]]]
  }

  def "updateByQuery retries transient poll failures like reindex"() {
    given:
    http.enqueue(200, [task: 'n:1'])
        .enqueue(503, [error: [type: 'unavailable', reason: 'busy']])
        .enqueueFailure(new IOException('connection reset'))
        .enqueue(200, [completed: true, response: [total: 2, updated: 2]])

    when:
    def result = engine.updateByQuery(UpdateByQueryRequest.of('docs'))

    then:
    result.updated() == 2
    http.requests*.path == ['/docs/_update_by_query'] + ['/_tasks/n:1'] * 3
  }

  def "an updateByQuery that times out is cancelled and named in the error"() {
    given:
    http.enqueue(200, [task: 'n:8'])
        .enqueue(200, [completed: false])
        .enqueue(200, [nodes: [:]])
        .enqueue(200, [completed: true, error: [type: 'task_cancelled_exception', reason: 'by user request']])

    when:
    engine.updateByQuery(UpdateByQueryRequest.of('docs').withTimeout(Duration.ofNanos(1)))

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.TIMEOUT
    e.message.startsWith('update by query task n:8 did not finish')
    e.message.contains('the task was cancelled')
    http.requests.collect { "${it.method} ${it.path}".toString() } == [
        'POST /docs/_update_by_query', 'GET /_tasks/n:8', 'POST /_tasks/n:8/_cancel', 'GET /_tasks/n:8']
  }

  def "a timeout names the last counters the task reported"() {
    given:
    http.enqueue(200, [task: 'n:9'])
        .enqueue(200, [completed: false, task: [status: [total: 100, created: 40, updated: 2, deleted: 0, noops: 1,
            version_conflicts: 3, batches: 5]]])
        .enqueue(200, [nodes: [:]])
        .enqueue(200, [completed: true])

    when:
    engine.reindex(ReindexRequest.of('a', 'b').withTimeout(Duration.ofNanos(1)))

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.TIMEOUT
    e.message.startsWith('reindex task n:9 did not finish within PT0.000000001S (last status: total=100, created=40,'
        + ' updated=2, deleted=0, noops=1, version_conflicts=3); ')
  }

  def "a conditional write without an id is refused before anything is sent"() {
    when:
    engine.index('docs', null, [a: 1], IndexOptions.ifMatch(1, 1))

    then:
    def e = thrown(IllegalArgumentException)
    e.message == 'a conditional write (ifSeqNo/ifPrimaryTerm) needs a document id'
    http.requests.isEmpty()
  }

  def "a failing updateByQuery poll cancels the task, and a task error is reported"() {
    given:
    def failing = new FakeHttp().enqueue(200, [task: 'n:1'])
        .enqueue(403, [error: [type: 'security_exception', reason: 'no']])
        .enqueue(404, [error: [type: 'resource_not_found_exception', reason: 'gone']])
        .enqueue(404, [error: [type: 'resource_not_found_exception', reason: 'gone']])
    def errored = new FakeHttp().enqueue(200, [task: 'n:2'])
        .enqueue(200, [completed: true, error: [reason: 'script_exception']])

    when:
    new JsonRestEngine(failing, Duration.ofMillis(1)).updateByQuery(UpdateByQueryRequest.of('docs'))

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.UNAUTHORIZED
    e.message.startsWith('polling update by query task n:1 failed')
    e.message.contains('the task was cancelled')

    when:
    new JsonRestEngine(errored, Duration.ofMillis(1)).updateByQuery(UpdateByQueryRequest.of('docs'))

    then:
    def error = thrown(EngineException)
    error.message == 'update by query task n:2 failed: script_exception'
  }

  def "putSettings sends the settings unchanged, and setWriteBlock goes through it"() {
    given:
    http.enqueue(200, [acknowledged: true]).enqueue(200, [acknowledged: true]).enqueue(200, [acknowledged: true])

    when:
    engine.putSettings('docs-v1', ['index.refresh_interval': '5s'])
    engine.putSettings('docs', [index: [number_of_replicas: 0]])
    engine.setWriteBlock('docs-v1', false)

    then:
    http.requests*.method == ['PUT', 'PUT', 'PUT']
    http.requests*.path == ['/docs-v1/_settings', '/docs/_settings', '/docs-v1/_settings']
    http.requests[0].json() == ['index.refresh_interval': '5s']
    http.requests[1].json() == [index: [number_of_replicas: 0]]
    http.requests[2].json() == ['index.blocks.write': false]
  }

  def "putSettings reports failures"() {
    given:
    http.enqueue(400, [error: [type: 'illegal_argument_exception', reason: 'unknown setting [index.nope]']])

    when:
    engine.putSettings('docs', ['index.nope': 1])

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.BAD_REQUEST
    e.message == 'illegal_argument_exception: unknown setting [index.nope]'
  }

  def "reindex surfaces failures, task errors and timeouts"() {
    given:
    def failing = new FakeHttp().enqueue(200, [task: 't']).enqueue(200, [completed: true, response: [total: 1, failures: [[id: '1', cause: [reason: 'boom']]]]])
    def errored = new FakeHttp().enqueue(200, [task: 't']).enqueue(200, [completed: true, error: [reason: 'script_exception']])
    def slow = new FakeHttp().enqueue(200, [task: 't']).enqueue(200, [completed: false])
        .enqueue(200, [acknowledged: true]).enqueue(200, [completed: true, error: [reason: 'cancelled']])

    expect:
    !new JsonRestEngine(failing, Duration.ofMillis(1)).reindex(ReindexRequest.of('a', 'b')).ok()
    new JsonRestEngine(failing, Duration.ofMillis(1)).reindexPollInterval() == Duration.ofMillis(1)

    when:
    new JsonRestEngine(errored, Duration.ofMillis(1)).reindex(ReindexRequest.of('a', 'b'))

    then:
    def errorEx = thrown(EngineException)
    errorEx.message.contains('script_exception')

    when:
    new JsonRestEngine(slow, Duration.ofMillis(1)).reindex(ReindexRequest.of('a', 'b').withTimeout(Duration.ofNanos(1)))

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.TIMEOUT
  }

  def "a reindex that times out is cancelled and awaited before the timeout is reported"() {
    given:
    http.enqueue(200, [task: 'n:7'])
        .enqueue(200, [completed: false])
        .enqueue(200, [nodes: [:]])
        .enqueue(200, [completed: false])
        .enqueue(200, [completed: true, error: [type: 'task_cancelled_exception', reason: 'by user request']])

    when:
    engine.reindex(ReindexRequest.of('a', 'b').withTimeout(Duration.ofNanos(1)))

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.TIMEOUT
    e.message.contains('did not finish')
    e.message.contains('the task was cancelled')
    http.requests.collect { "${it.method} ${it.path}".toString() } == [
        'POST /_reindex', 'GET /_tasks/n:7', 'POST /_tasks/n:7/_cancel', 'GET /_tasks/n:7', 'GET /_tasks/n:7']
  }

  def "transient poll failures are retried and the reindex then succeeds"() {
    given:
    http.enqueue(200, [task: 'n:1'])
        .enqueue(503, [error: [type: 'unavailable', reason: 'busy']])
        .enqueueFailure(new IOException('connection reset'))
        .enqueue(502, 'bad gateway')
        .enqueue(200, [completed: true, response: [total: 1, created: 1, failures: []]])

    when:
    def result = engine.reindex(ReindexRequest.of('a', 'b'))

    then:
    result.ok()
    result.created() == 1
    http.requests*.path == ['/_reindex'] + ['/_tasks/n:1'] * 4
  }

  def "a poll that keeps failing cancels the task after the retries"() {
    given:
    http.enqueue(200, [task: 'n:1'])
    4.times { http.enqueue(503, [error: [type: 'unavailable', reason: 'busy']]) }
    http.enqueue(200, [acknowledged: true])
        .enqueue(500, 'still broken')
        .enqueue(200, [completed: true])

    when:
    engine.reindex(ReindexRequest.of('a', 'b'))

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.TRANSIENT
    e.status == 503
    e.message.contains('polling reindex task n:1 failed')
    e.message.contains('the task was cancelled')
    http.requests.collect { "${it.method} ${it.path}".toString() } ==
        ['POST /_reindex'] + ['GET /_tasks/n:1'] * 4 + ['POST /_tasks/n:1/_cancel', 'GET /_tasks/n:1', 'GET /_tasks/n:1']
  }

  def "a non-transient poll failure cancels at once; a task already gone counts as cancelled"() {
    given:
    http.enqueue(200, [task: 'n:1'])
        .enqueue(403, [error: [type: 'security_exception', reason: 'no']])
        .enqueue(404, [error: [type: 'resource_not_found_exception', reason: 'gone']])
        .enqueue(404, [error: [type: 'resource_not_found_exception', reason: 'gone']])

    when:
    engine.reindex(ReindexRequest.of('a', 'b'))

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.UNAUTHORIZED
    e.message.contains('the task was cancelled')
    http.requests.size() == 4
  }

  def "a failed cancel is reported as such"() {
    given:
    http.enqueue(200, [task: 'n:1'])
        .enqueue(400, [error: [type: 'illegal_argument_exception', reason: 'bad']])
        .enqueue(500, 'cancel broke')

    when:
    engine.reindex(ReindexRequest.of('a', 'b'))

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.BAD_REQUEST
    e.message.contains('cancelling the task failed')
    e.message.contains('may still be running')
  }

  def "a cancelled task that does not stop within the cancel wait is reported as possibly running"() {
    given:
    http.enqueue(200, [task: 'n:1'])
        .enqueue(200, [completed: false])
        .enqueue(200, [nodes: [:]])
        .enqueue(500, 'flaky')
    def impatient = new JsonRestEngine(http, Duration.ofMillis(1), Duration.ofNanos(1))

    when:
    impatient.reindex(ReindexRequest.of('a', 'b').withTimeout(Duration.ofNanos(1)))

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.TIMEOUT
    e.message.contains('asked to cancel but had not stopped within PT0.000000001S')
  }

  def "an interrupt while waiting for a cancelled task ends the wait"() {
    given:
    http.enqueue(200, [task: 'n:1'])
        .enqueue(403, [error: [type: 'security_exception', reason: 'no']])
        .enqueue(200, [nodes: [:]])
        .enqueue(200, [completed: false])

    when:
    Thread.currentThread().interrupt()
    engine.reindex(ReindexRequest.of('a', 'b'))

    then:
    def e = thrown(EngineException)
    e.message.contains('interrupted while waiting for it to stop')
    Thread.interrupted()
  }

  def "a failed cancel after an interrupt is attached to the interruption"() {
    given:
    http.enqueue(200, [task: 'n:1']).enqueue(200, [completed: false]).enqueueFailure(new IOException('down'))

    when:
    Thread.currentThread().interrupt()
    new JsonRestEngine(http, Duration.ofSeconds(30)).reindex(ReindexRequest.of('a', 'b'))

    then:
    def e = thrown(EngineException)
    e.message.contains('Interrupted')
    e.suppressed*.message.any { it.contains('down') }
    Thread.interrupted()
  }

  def "interrupting the wait cancels the task without waiting for it"() {
    given:
    http.enqueue(200, [task: 'n:1']).enqueue(200, [completed: false]).enqueue(200, [acknowledged: true])
    def slowEngine = new JsonRestEngine(http, Duration.ofSeconds(30))

    when:
    Thread.currentThread().interrupt()
    slowEngine.reindex(ReindexRequest.of('a', 'b'))

    then:
    def e = thrown(EngineException)
    e.message.contains('Interrupted')
    Thread.interrupted()
    http.requests.last().path == '/_tasks/n:1/_cancel'
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
    http.requests[3].params == [retry_on_conflict: '3']

    and:
    engine.delete('docs', '1')
    !engine.delete('docs', '9')
    http.requests[4].method == 'DELETE' && http.requests[4].path == '/docs/_doc/1' && http.requests[4].params == [:]
  }

  def "update takes a retry_on_conflict, 0 sending none"() {
    given:
    http.enqueue(200, [_index: 'd', _id: '1', result: 'updated']).enqueue(200, [_index: 'd', _id: '1', result: 'updated'])

    when:
    engine.update('d', '1', [a: 1], 0)
    engine.update('d', '1', [a: 1], 7)

    then:
    http.requests*.params == [[:], [retry_on_conflict: '7']]

    when:
    engine.update('d', '1', [a: 1], -1)

    then:
    thrown(IllegalArgumentException)
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

  def "writes return the sequence number and primary term the engine reports"() {
    given:
    http.enqueue(201, [_index: 'docs-v1', _id: '1', result: 'created', _version: 1, _seq_no: 12, _primary_term: 3])
        .enqueue(200, [_index: 'docs-v1', _id: '1', result: 'updated', _version: 2, _seq_no: 13, _primary_term: 3])
        .enqueue(200, [_index: 'docs-v1', _id: '1', result: 'updated', _version: 3])

    when:
    def indexed = engine.index('docs', '1', [a: 1], IndexOptions.defaults())
    def updated = engine.update('docs', '1', [a: 2])
    def old = engine.index('docs', '1', [a: 3], IndexOptions.defaults())

    then:
    indexed.seqNo() == 12L
    indexed.primaryTerm() == 3L
    updated.seqNo() == 13L
    updated.primaryTerm() == 3L
    old.seqNo() == null
    old.primaryTerm() == null
  }

  def "conditional index and delete send if_seq_no and if_primary_term, and a mismatch is a CONFLICT"() {
    given:
    http.enqueue(200, [_index: 'docs-v1', _id: '1', result: 'updated', _version: 2, _seq_no: 5, _primary_term: 1])
        .enqueue(200, [_index: 'docs-v1', _id: '1', result: 'deleted', _version: 3])
        .enqueue(409, [error: [type: 'version_conflict_engine_exception',
            reason: '[1]: version conflict, required seqNo [4], primary term [1]. current document has seqNo [5] and primary term [1]'],
            status: 409])

    when:
    engine.index('docs', '1', [a: 1], IndexOptions.ifMatch(4, 1).withRefresh(IndexOptions.Refresh.WAIT_FOR))
    engine.delete('docs', '1', IndexOptions.ifMatch(5, 1))

    then:
    http.requests[0].method == 'PUT'
    http.requests[0].path == '/docs/_doc/1'
    http.requests[0].params == [if_primary_term: '1', if_seq_no: '4', refresh: 'wait_for']
    http.requests[1].method == 'DELETE'
    http.requests[1].params == [if_primary_term: '1', if_seq_no: '5']

    when:
    engine.index('docs', '1', [a: 2], IndexOptions.ifMatch(4, 1))

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.CONFLICT
    e.status == 409
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
    hit.get().version() == 3L
    hit.get().seqNo() == 4L
    hit.get().primaryTerm() == 1L
    hit.get().highlight() == [:]
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

  def "bulk puts if_seq_no and if_primary_term into the action metadata of conditional operations"() {
    given:
    http.enqueue(200, [took: 1, errors: true, items: [
        [index: [_index: 'docs-v1', _id: '1', status: 200, _seq_no: 9, _primary_term: 2]],
        [delete: [_index: 'docs-v1', _id: '2', status: 409, error: [type: 'version_conflict_engine_exception', reason: 'stale']]],
    ]])

    when:
    def result = engine.bulk([
        BulkOp.indexIfMatch('docs', '1', [a: 1], 8, 2),
        BulkOp.deleteIfMatch('docs', '2', 3, 1),
    ])

    then:
    http.last.body == '{"index":{"_id":"1","_index":"docs","if_primary_term":2,"if_seq_no":8}}\n{"a":1}\n' +
        '{"delete":{"_id":"2","_index":"docs","if_primary_term":1,"if_seq_no":3}}\n'
    result.failures()*.id() == ['2']
    result.failures()[0].status() == 409
  }

  def "search reads version, sequence number, primary term and highlights when the engine returns them"() {
    given:
    http.enqueue(200, [took: 1, hits: [total: [value: 2, relation: 'eq'], hits: [
        [_index: 'docs-v1', _id: '1', _version: 4, _seq_no: 17, _primary_term: 2, _score: 1.0, _source: [a: 1],
            highlight: [title: ['<em>merger</em> agreement'], body: ['a <em>merger</em>', 'the <em>merger</em>']]],
        [_index: 'docs-v1', _id: '2', _score: 0.5, _source: [a: 2]],
    ]]])

    when:
    def result = engine.search(['docs'], [version: true, seq_no_primary_term: true, highlight: [fields: [title: [:], body: [:]]]])

    then:
    http.last.json() == [highlight: [fields: [body: [:], title: [:]]], seq_no_primary_term: true, version: true]
    def first = result.hits()[0]
    first.version() == 4L
    first.seqNo() == 17L
    first.primaryTerm() == 2L
    first.highlight() == [body: ['a <em>merger</em>', 'the <em>merger</em>'], title: ['<em>merger</em> agreement']]
    def second = result.hits()[1]
    second.version() == null
    second.seqNo() == null
    second.primaryTerm() == null
    second.highlight() == [:]
  }

  def "a highlight entry that is not a list has no fragments"() {
    given:
    http.enqueue(200, [hits: [total: [value: 1, relation: 'eq'], hits: [
        [_index: 'i', _id: '1', _source: [:], highlight: [odd: 'text', some: ['x', null]]]]]])

    expect:
    engine.search(['i'], [:]).hits()[0].highlight() == [odd: [], some: ['x', null]]
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

  def "a search without hits.total has no total"() {
    given: 'the response to track_total_hits: false'
    http.enqueue(200, [took: 1, hits: [max_score: null, hits: [[_index: 'i', _id: '1', _source: [a: 1]]]]])

    when:
    def result = engine.search(['a'], [track_total_hits: false])

    then:
    result.total() == SearchResult.NO_TOTAL
    result.total() == -1
    !result.totalExact()
    !result.hasTotal()
    result.hits()*.id() == ['1']
  }

  def "a search some shards failed reports them, with the other shards' results"() {
    given: 'the engines answer 200 with _shards.failed and an exact-looking total'
    http.enqueue(200, [took: 1, _shards: [total: 3, successful: 2, failed: 1,
                                          failures: [[shard: 0, reason: [type: 'search_context_missing_exception']]]],
                       hits: [total: [value: 4, relation: 'eq'], hits: []]])
    http.enqueue(200, [took: 1, _shards: [total: 3, successful: 3, failed: 0], hits: [total: [value: 4, relation: 'eq'], hits: []]])

    expect:
    with(engine.search(['a'], [size: 0])) {
      shardsFailed() == 1
      shardFailures() == ['search_context_missing_exception']
      partial()
      total() == 4L
      totalExact()
    }
    !engine.search(['a'], [size: 0]).partial()
    new SearchResult(0, true, [], null, [:], 0).shardsFailed() == 0
    new SearchResult(0, true, [], null, [:], 0, 2).shardFailures().isEmpty()
    new SearchResult(0, true, [], null, [:], 0, 1, null).shardFailures().isEmpty()
  }

  def "null sort values, as returned for a missing sort field, are kept"() {
    given:
    http.enqueue(200, [took: 1, hits: [total: [value: 1, relation: 'eq'],
        hits: [[_index: 'i', _id: '1', _source: [a: 1], sort: [null, 'doc-1', 7]]]]])

    when:
    def result = engine.search(['a'], [:])

    then:
    result.hits()[0].sort() == [null, 'doc-1', 7]
    result.lastSort() == [null, 'doc-1', 7]
  }

  def "a numeric hits.total is exact"() {
    given: 'the response shape of rest_total_hits_as_int=true'
    http.enqueue(200, [hits: [total: 7, hits: []]])

    when:
    def result = engine.search(['a'], [:])

    then:
    result.total() == 7
    result.totalExact()
    result.hasTotal()
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
    503    | 'cluster_block_exception'           | EngineException.Kind.TRANSIENT
    500    | 'null_pointer_exception'            | EngineException.Kind.OTHER
  }

  @Unroll
  def "#status #type is BLOCKED, not an authorization failure or transient"() {
    given:
    http.enqueue(status, [error: [type: type, reason: 'index [x] blocked by: [FORBIDDEN/8/index write (api)];']])

    when:
    engine.index('x', '1', [a: 1], IndexOptions.defaults())

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.BLOCKED
    e.status == status
    e.message.startsWith("${type}: index [x] blocked by")
    e.message.endsWith('(blocked: lift the block before retrying)')

    where:
    status | type
    403    | 'cluster_block_exception'
    429    | 'cluster_block_exception'
    400    | 'index_closed_exception'
  }

  def "a BLOCKED reindex poll is not retried"() {
    given:
    http.enqueue(200, [task: 'n:1'])
        .enqueue(403, [error: [type: 'cluster_block_exception', reason: 'blocked']])
        .enqueue(404, [error: [type: 'resource_not_found_exception', reason: 'gone']])
        .enqueue(404, [error: [type: 'resource_not_found_exception', reason: 'gone']])

    when:
    engine.reindex(ReindexRequest.of('a', 'b'))

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.BLOCKED
    http.requests.size() == 4
  }

  def "a 503 cluster block during a reindex poll is retried"() {
    given:
    http.enqueue(200, [task: 'n:1'])
        .enqueue(503, [error: [type: 'cluster_block_exception', reason: 'state not recovered']])
        .enqueue(200, [completed: true, response: [total: 1, created: 1, failures: []]])

    when:
    def result = engine.reindex(ReindexRequest.of('a', 'b'))

    then:
    result.created() == 1
    http.requests.size() == 3
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
