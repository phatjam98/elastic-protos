package com.phatjam98.elasticprotos.query

import com.phatjam98.elasticprotos.engine.SearchEngine
import com.phatjam98.elasticprotos.engine.SearchResult
import com.phatjam98.elasticprotos.mapping.IndexDefinition
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.CalendarInterval
import com.phatjam98.elasticprotos.proto.SearchCriteria
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.util.Json
import spock.lang.Shared
import spock.lang.Specification

class CompositePagerSpec extends Specification {

  @Shared IndexDefinition document = ProtoMapper.withDefaults().map(Document)
  QueryCompiler compiler = QueryCompiler.withDefaults()

  /** The recorded pages of composite 'pairs', see AggregationResultsSpec.pages(). */
  static Map<String, Map> recorded() {
    Json.parseObject(new File('src/test/resources/responses/composite-pages.json').text)
  }

  static SearchResult result(Map aggs) {
    new SearchResult(6, true, [], null, aggs, 1)
  }

  static SearchCriteria criteria() {
    def pairs = Aggs.composite('pairs', Aggs.termsSource('firm', 'firm_id'),
        Aggs.dateHistogramSource('month', 'filed_at', CalendarInterval.MONTH).tap { dateHistogramBuilder.setFormat('yyyy-MM') },
        Aggs.termsSource('relevance', 'relevance').setMissingBucket(true))
    pairs.compositeBuilder.setSize(4)
    Criteria.where('type').anyOf('FILING', 'MOTION', 'ORDER', 'NOTICE').end()
        .aggregate(Aggs.sub(pairs, Aggs.sum('total', 'size_bytes')), Aggs.terms('other', 'tags'))
        .highlight('title')
        .pageSize(50)
        .build()
  }

  def "pages through every bucket, passing each after_key on, and stops at the page without one"() {
    given:
    SearchEngine engine = Mock()
    List<Map> bodies = []
    def pages = recorded()

    when:
    def pager = CompositePager.open(engine, compiler, ['documents'], criteria(), document, 'pairs')
    List<List<AggregationResults.Bucket>> seen = []
    while (pager.hasNext()) {
      seen << pager.next()
    }

    then:
    3 * engine.search(['documents'], _) >> { args ->
      bodies << args[1]
      result(pages["page${bodies.size()}"])
    }
    seen*.size() == [4, 2]
    seen.flatten()*.compositeKey()*.firm == ['firm-1', 'firm-1', 'firm-2', 'firm-2', 'firm-3', 'firm-3']
    !pager.hasNext()
    pager.afterKey() == Optional.of([firm: 'firm-3', month: '2024-06', relevance: 0.75d])

    and: 'only the composite, no hits, no hit count, no highlight, the query kept, and the after key of the previous page'
    bodies.every { it.size == 0 && !it.containsKey('highlight') && it.aggs.keySet() == ['pairs'] as Set }
    bodies.every { it.track_total_hits == false }
    pager.warnings().isEmpty()
    bodies.every { it.query == [bool: [filter: [[terms: [type: ['FILING', 'MOTION', 'ORDER', 'NOTICE']]]]]] }
    bodies*.aggs*.pairs*.composite*.after == [null, [firm: 'firm-2', month: '2024-03', relevance: null],
                                              [firm: 'firm-3', month: '2024-06', relevance: 0.75d]]
    bodies.every { it.aggs.pairs.aggs == [total: [sum: [field: 'size_bytes']]] }

    when:
    pager.next()

    then:
    thrown(NoSuchElementException)
  }

  def "a first page without buckets ends at once, and a page without an after key is the last"() {
    given:
    SearchEngine engine = Mock()

    when:
    def empty = CompositePager.open(engine, compiler, ['documents'], criteria(), document, 'pairs')

    then:
    empty.afterKey() == Optional.empty()

    when:
    def hasNext = empty.hasNext()

    then:
    1 * engine.search(_, _) >> result([pairs: [buckets: []]])
    !hasNext
    !empty.hasNext()

    when: 'buckets but no after key'
    def single = CompositePager.open(engine, compiler, ['documents'], criteria(), document, 'pairs')
    def page = single.next()

    then:
    1 * engine.search(_, _) >> result([pairs: [buckets: [[key: [firm: 'a', month: 'b', relevance: null], doc_count: 1]]]])
    page*.docCount() == [1L]
    !single.hasNext()
    single.afterKey() == Optional.empty()
  }

  def "forEachBucket hands every bucket over, and a nested composite is found behind its wrapper"() {
    given:
    SearchEngine engine = Mock()
    def roles = Criteria.builder().aggregate(Aggs.composite('roles', Aggs.termsSource('role', 'parties.role'))).build()
    List<Object> keys = []

    when:
    CompositePager.forEachBucket(engine, compiler, ['documents'], roles, document, 'roles') { keys << it.key() }

    then:
    2 * engine.search(_, _) >>> [
        result([roles__nested: [doc_count: 8, roles: [after_key: [role: 'PLAINTIFF'], buckets: [[key: [role: 'COUNSEL'], doc_count: 1], [key: [role: 'PLAINTIFF'], doc_count: 3]]]]]),
        result([roles__nested: [doc_count: 8, roles: [buckets: []]]])]
    keys == [[role: 'COUNSEL'], [role: 'PLAINTIFF']]
  }

  def "an after key already set is where the first page starts"() {
    given:
    SearchEngine engine = Mock()
    def criteria = criteria().toBuilder()
    criteria.setAggregation(0, Aggs.after(criteria.getAggregation(0).toBuilder(), [firm: 'firm-2', month: '2024-03', relevance: null]))

    when:
    CompositePager.open(engine, compiler, ['documents'], criteria.build(), document, 'pairs').hasNext()

    then:
    1 * engine.search(_, _) >> { args ->
      assert args[1].aggs.pairs.composite.after == [firm: 'firm-2', month: '2024-03', relevance: null]
      result([pairs: [buckets: []]])
    }
  }

  def "an engine that returns the after key it was sent makes the pager throw instead of looping"() {
    given:
    SearchEngine engine = Mock()
    def page = [pairs: [after_key: [firm: 'a', month: 'b', relevance: null],
                        buckets: [[key: [firm: 'a', month: 'b', relevance: null], doc_count: 1]]]]

    when:
    def pager = CompositePager.open(engine, compiler, ['documents'], criteria(), document, 'pairs')
    pager.next()
    pager.hasNext()

    then:
    2 * engine.search(_, _) >> result(page)
    def e = thrown(IllegalStateException)
    e.message.startsWith("Composite aggregation 'pairs' returned the after key it was sent")
    !pager.hasNext()
  }

  def "a composite a lenient compiler skipped fails when the pager is opened; invalid requests too"() {
    given:
    def lenient = QueryCompiler.of(QueryCompilerConfig.standard().withLenient(true))
    def unknown = Criteria.builder().aggregate(Aggs.composite('c', Aggs.termsSource('a', 'nope'))).build()

    when:
    CompositePager.open(Mock(SearchEngine), lenient, ['documents'], unknown, document, 'c')

    then:
    def e = thrown(IllegalArgumentException)
    e.message == "Composite aggregation 'c' was skipped by the lenient compiler, so there is nothing to page: [Skipped composite aggregation 'c' on unknown field 'nope']"

    when:
    CompositePager.open(Mock(SearchEngine), compiler, ['documents'], unknown, document, 'c')

    then:
    thrown(QueryCompileException)

    expect: 'only the paged request is compiled: other aggregations are not checked'
    CompositePager.open(Mock(SearchEngine), lenient, ['documents'], criteria().toBuilder()
        .addAggregation(Aggs.terms('t', 'nope').build()).build(), document, 'pairs').warnings().isEmpty()
  }

  def "open refuses criteria without that composite aggregation"() {
    when:
    CompositePager.open(Mock(SearchEngine), compiler, ['documents'], criteria, document, 'pairs')

    then:
    def e = thrown(IllegalArgumentException)
    e.message == message

    where:
    criteria                                                                || message
    SearchCriteria.defaultInstance                                          || "The criteria have no top-level aggregation named 'pairs'"
    Criteria.builder().aggregate(Aggs.terms('pairs', 'type')).build()       || "Aggregation 'pairs' is a TERMS aggregation, not a composite one"
  }
}
