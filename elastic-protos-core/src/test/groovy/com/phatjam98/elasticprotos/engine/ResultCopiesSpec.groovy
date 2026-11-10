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

  def "the five-argument hit has no version metadata and no highlights"() {
    when:
    def hit = new Hit('i', 'id', 1.0d, [a: 1], [])

    then:
    hit.version() == null
    hit.seqNo() == null
    hit.primaryTerm() == null
    hit.highlight() == [:]
    hit.nestedHighlights() == NestedHighlights.empty()
    hit == new Hit('i', 'id', 1.0d, [a: 1], [], null, null, null, [:])
    hit == new Hit('i', 'id', 1.0d, [a: 1], [], null, null, null, [:], NestedHighlights.empty())
    new Hit('i', 'id', 1.0d, [a: 1], [], null, null, null, [:], null).nestedHighlights().isEmpty()
  }

  def "nested highlights copy their parts, map nulls to empty, and read fragments across objects"() {
    given:
    def levels = [new NestedLevel('a', 0), new NestedLevel('b', 2)]
    def fragments = ['<em>x</em>']
    def nested = new NestedHighlight(levels, ['a.b.c': fragments, 'a.b.d': null])
    def objects = [nested]
    def totals = [new NestedTotal([new NestedLevel('a', 0)], 'a.b', 4, 1, true)]
    def hit = new Hit('i', 'id', null, [:], [], null, null, null, [t: ['root']], NestedHighlights.of(objects, totals))

    when:
    levels << new NestedLevel('c', 9)
    fragments << 'y'
    objects << nested
    totals.clear()

    then:
    hit.nestedHighlights().objects() == [new NestedHighlight([new NestedLevel('a', 0), new NestedLevel('b', 2)],
        ['a.b.c': ['<em>x</em>'], 'a.b.d': []])]
    hit.nestedHighlights().objects()[0].path() == 'a.b'
    hit.nestedHighlights().objects()[0].offsets() == [0, 2]
    hit.nestedHighlights().totals()*.truncated() == [true]
    hit.nestedHighlights().isTruncated()
    !hit.nestedHighlights().isEmpty()
    hit.fragments('t') == ['root']
    hit.fragments('a.b.c') == ['<em>x</em>']
    hit.fragments('nope') == []
    hit.nestedHighlights() == NestedHighlights.of([nested], [new NestedTotal([new NestedLevel('a', 0)], 'a.b', 4, 1, true)])
    hit.nestedHighlights().hashCode() == NestedHighlights.of([nested], hit.nestedHighlights().totals()).hashCode()
    hit.nestedHighlights().toString().startsWith('NestedHighlights[objects=[NestedHighlight[levels=')
    hit.nestedHighlights() != NestedHighlights.empty()
    hit.nestedHighlights() != 'x'

    and: 'nulls become empty'
    new NestedHighlight(null, null) == new NestedHighlight([], [:])
    new NestedTotal(null, 'p', 0, 0, false).parent() == []
    NestedHighlights.of(null, null).is(NestedHighlights.empty())
    NestedHighlights.of([], []).is(NestedHighlights.empty())
    NestedHighlights.of(null, totals + [new NestedTotal([], 'p', 1, 1, false)]).objects() == []

    when:
    hit.nestedHighlights().objects().add(nested)

    then:
    thrown(UnsupportedOperationException)

    when:
    hit.nestedHighlights().objects()[0].levels().add(new NestedLevel('z', 1))

    then:
    thrown(UnsupportedOperationException)

    when:
    new NestedLevel(null, 0)

    then:
    thrown(NullPointerException)

    when:
    new NestedTotal([], null, 0, 0, false)

    then:
    thrown(NullPointerException)
  }

  def "a hit copies its highlights and each fragment list, and a null highlight is empty"() {
    given:
    def fragments = ['<em>a</em>', 'b']
    def highlight = [title: fragments, body: null]
    def hit = new Hit('i', 'id', null, [:], [], 3L, 7L, 1L, highlight)

    when:
    fragments << 'c'
    highlight.extra = ['x']

    then:
    hit.version() == 3L
    hit.seqNo() == 7L
    hit.primaryTerm() == 1L
    hit.highlight() == [title: ['<em>a</em>', 'b'], body: []]
    hit.highlight().keySet().toList() == ['title', 'body']
    new Hit('i', 'id', null, null, null, null, null, null, null).highlight() == [:]

    when:
    hit.highlight().put('more', [])

    then:
    thrown(UnsupportedOperationException)

    when:
    hit.highlight().title.add('d')

    then:
    thrown(UnsupportedOperationException)
  }

  def "the four-argument index result has no sequence number or primary term"() {
    expect:
    new IndexResult('i', '1', 'created', 1).seqNo() == null
    new IndexResult('i', '1', 'created', 1).primaryTerm() == null
    new IndexResult('i', '1', 'created', 1, 5L, 2L).seqNo() == 5L
    new IndexResult('i', '1', 'created', 1, 5L, 2L).primaryTerm() == 2L
  }

  def "the six-argument reindex result has no noops, deletions or conflicts, and accounted adds them all up"() {
    given:
    def legacy = new ReindexResult(5, 3, 2, [], 1, false)

    expect:
    legacy.noops() == 0
    legacy.deleted() == 0
    legacy.versionConflicts() == 0
    legacy.accounted()
    legacy == new ReindexResult(5, 3, 2, [], 1, false, 0, 0, 0)
    new ReindexResult(10, 1, 2, [], 0, false, 3, 2, 2).accounted()
    !new ReindexResult(10, 1, 2, [], 0, false, 3, 2, 1).accounted()
    !new ReindexResult(4, 3, 0, [], 0, false).accounted()
  }

  def "complete needs no failures, no timeout and every document accounted for"() {
    expect:
    new ReindexResult(2, 2, 0, [], 0, false).complete()
    !new ReindexResult(3, 2, 0, [], 0, false).complete()
    !new ReindexResult(2, 2, 0, [], 0, true).complete()
    !new ReindexResult(2, 1, 0, [[id: '2']], 0, false, 0, 0, 1).complete()
    new ReindexResult(2, 1, 0, [[id: '2']], 0, false, 0, 0, 1).accounted()
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
