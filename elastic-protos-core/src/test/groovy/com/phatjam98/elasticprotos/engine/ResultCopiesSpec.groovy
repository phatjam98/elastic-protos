package com.phatjam98.elasticprotos.engine

import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.migration.Action
import com.phatjam98.elasticprotos.migration.IndexPlan
import com.phatjam98.elasticprotos.migration.MappingDiff
import com.phatjam98.elasticprotos.migration.MigrationResult
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.query.CompiledSearch
import spock.lang.Specification

/** Result types copy their collections on construction and hand out unmodifiable views. */
class ResultCopiesSpec extends Specification {

  def "a hit copies its document and sort values, which may hold nulls"() {
    given:
    def nested = [inner: 1]
    def source = [a: 1, b: null, n: nested]
    def sort = ['x', null, 3]
    def hit = new Hit('i', 'id', 1.0d, source, sort)

    when:
    source.a = 2
    source.c = 3
    sort << 'y'
    sort[0] = 'z'

    then:
    hit.source() == [a: 1, b: null, n: [inner: 1]]
    hit.sort() == ['x', null, 3]
    hit.source().keySet().toList() == ['a', 'b', 'n']

    when: 'the copy is one level deep'
    nested.inner = 2

    then:
    hit.source().n == [inner: 2]

    when:
    hit.source().put('d', 4)

    then:
    thrown(UnsupportedOperationException)

    when:
    hit.sort().add('w')

    then:
    thrown(UnsupportedOperationException)
  }

  def "a hit keeps a null document or sort"() {
    expect:
    new Hit('i', 'id', null, null, null).source() == null
    new Hit('i', 'id', null, null, null).sort() == null
  }

  def "a search result copies its hits and aggregations"() {
    given:
    def hits = [new Hit('i', '1', null, [:], [1])]
    def aggregations = [count: [value: 1], none: null]
    def result = new SearchResult(1, true, hits, null, aggregations, 0)

    when:
    hits << new Hit('i', '2', null, [:], [2])
    aggregations.more = [:]

    then:
    result.hits()*.id() == ['1']
    result.aggregations() == [count: [value: 1], none: null]
    result.lastSort() == [1]

    when:
    result.hits().clear()

    then:
    thrown(UnsupportedOperationException)

    when:
    result.aggregations().remove('count')

    then:
    thrown(UnsupportedOperationException)
  }

  def "null collections in a search result become empty"() {
    when:
    def result = new SearchResult(0, true, null, null, null, 0)

    then:
    result.hits() == []
    result.aggregations() == [:]
    result.lastSort() == null
  }

  def "a bulk result copies its items"() {
    given:
    def items = [new BulkResult.Item('index', 'i', '1', 201, null)]
    def result = new BulkResult(false, items, 0)

    when:
    items << new BulkResult.Item('index', 'i', '2', 400, 'bad')

    then:
    result.items()*.id() == ['1']
    result.failures().isEmpty()
    new BulkResult(false, null, 0).items() == []

    when:
    result.items().add(null)

    then:
    thrown(UnsupportedOperationException)
  }

  def "a reindex result copies its failures"() {
    given:
    def failures = [[reason: 'x', cause: null]]
    def result = new ReindexResult(1, 0, 0, failures, 0, false)

    when:
    failures << [reason: 'y']

    then:
    result.failures() == [[reason: 'x', cause: null]]
    !result.ok()
    new ReindexResult(0, 0, 0, null, 0, false).ok()

    when:
    result.failures().remove(0)

    then:
    thrown(UnsupportedOperationException)
  }

  def "a compiled search copies its body in order"() {
    given:
    def body = new LinkedHashMap([query: [match_all: [:]], size: 10, search_after: null])
    def compiled = new CompiledSearch(body, [], false)

    when:
    body.size = 20
    body.from = 0

    then:
    compiled.body() == [query: [match_all: [:]], size: 10, search_after: null]
    compiled.body().keySet().toList() == ['query', 'size', 'search_after']

    when:
    compiled.body().put('aggs', [:])

    then:
    thrown(UnsupportedOperationException)
  }

  def "mapping diffs and migration outcomes copy their lists"() {
    given:
    def added = [new MappingDiff.Change('a', null, 'keyword')]
    def diff = new MappingDiff(added, [], [], [], false)
    def plan = new IndexPlan(ProtoMapper.withDefaults().map(Firm), Action.CREATE, null, null, diff,
        'r', [])
    def retained = ['old']
    def warnings = ['w']
    def outcome = new MigrationResult.Outcome(plan, MigrationResult.Status.APPLIED, 'new', retained,
        null, 1L, 1L, warnings)

    when:
    added << new MappingDiff.Change('b', null, 'long')
    retained << 'older'
    warnings << 'x'

    then:
    diff.added()*.path() == ['a']
    outcome.retainedIndices() == ['old']
    outcome.warnings() == ['w']

    when:
    outcome.warnings().add('y')

    then:
    thrown(UnsupportedOperationException)

    when:
    diff.added().clear()

    then:
    thrown(UnsupportedOperationException)
  }
}
