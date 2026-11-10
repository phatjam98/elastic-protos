package com.phatjam98.elasticprotos.query

import com.phatjam98.elasticprotos.engine.SearchResult
import com.phatjam98.elasticprotos.util.Json
import spock.lang.Specification

/**
 * Reads one normalised fixture: the {@code aggregations} object of a search over the testkit sample
 * documents (plus a terms aggregation on a small boolean index), as parsed by the engine adapter
 * and re-serialised with sorted keys. Recorded from Elasticsearch 8.19.23 and 9.5.5 and OpenSearch
 * 2.19.7 and 3.9.0, which produced identical normalised output; it covers keyed buckets, a null
 * metric, boolean and date terms, and nested / reverse_nested wrappers.
 */
class AggregationResultsSpec extends Specification {

  static AggregationResults sample() {
    AggregationResults.of(Json.parseObject(
        new File('src/test/resources/responses/aggregations-sample.json').text))
  }

  def "names unwrap the nested wrappers"() {
    when:
    def aggs = sample()

    then:
    aggs.names() == ['bands', 'by_type', 'counted', 'days', 'flags', 'keyed_bands', 'keyed_months', 'latest',
                     'per_quarter', 'quarters', 'relevances', 'roles', 'sizes', 'sizes_terms'] as Set
    aggs.has('roles')
    !aggs.has('firms')
    !aggs.has('nope')
    !aggs.isEmpty()
    aggs.raw().containsKey('roles__nested')
  }

  def "terms buckets with metric sub-aggregations, a null metric and the other count"() {
    when:
    def aggs = sample()
    def byType = aggs.buckets('by_type')

    then:
    byType*.key() == ['FILING', 'MOTION']
    byType*.docCount() == [3L, 1L]
    byType*.keyAsString() == [null, null]
    byType*.keyString() == ['FILING', 'MOTION']
    aggs.sumOtherDocCount('by_type') == 2
    byType[0].sub().names() == ['lowest_relevance', 'total'] as Set
    byType[0].sub().metric('total').value() == 2_870_000d
    byType[0].sub().metric('lowest_relevance') == new AggregationResults.MetricValue(0.75d, null)
    !byType[1].sub().metric('lowest_relevance').isPresent()
    byType[1].sub().metric('lowest_relevance').value() == null

    and: 'numeric terms keys are Long, floating-point ones Double'
    aggs.buckets('sizes_terms')*.key() == [8000L, 30000L]
    aggs.buckets('sizes_terms')[0].key() instanceof Long
    aggs.buckets('relevances')*.key() == [0.75d]

    and: 'boolean terms have Long keys 1 / 0, date terms epoch millis, both with key_as_string'
    aggs.buckets('flags')*.key() == [1L, 0L]
    aggs.buckets('flags').every { it.key() instanceof Long }
    aggs.buckets('flags')*.keyAsString() == ['true', 'false']
    aggs.buckets('flags')*.docCount() == [2L, 1L]
    aggs.buckets('days')*.key() == [1705314600250L, 1708437600000L]
    aggs.buckets('days').every { it.key() instanceof Long }
    aggs.buckets('days')*.keyAsString() == ['2024-01-15T10:30:00.250Z', '2024-02-20T14:00:00.000Z']
    aggs.sumOtherDocCount('days') == 4
  }

  def "nested terms and reverse-nested metrics are unwrapped"() {
    when:
    def roles = sample().buckets('roles')

    then:
    roles*.key() == ['DEFENDANT', 'PLAINTIFF', 'COUNSEL']
    roles*.docCount() == [4L, 3L, 1L]
    roles*.sub()*.metric('firms')*.asLong() == [3L, 2L, 1L]
    roles[0].sub().names() == ['firms', 'latest'] as Set
    roles[0].sub().metric('latest').valueAsString() == '2024-06-30T08:00:00.000Z'
    roles[0].sub().metric('latest').asLong() == 1719734400000L
  }

  def "histograms, date histograms, ranges and keyed buckets"() {
    when:
    def aggs = sample()

    then: 'date histogram keys are epoch millis'
    aggs.buckets('per_quarter')*.key() == [1704067200000L, 1711929600000L]
    aggs.buckets('per_quarter')*.keyAsString() == ['2024-01-01T00:00:00.000Z', '2024-04-01T00:00:00.000Z']
    aggs.buckets('per_quarter')[0].sub().metric('average').value() == 65000d

    and: 'histogram keys are Double'
    aggs.buckets('sizes')*.key() == [0d, 1_000_000d, 2_000_000d]
    aggs.buckets('sizes')*.docCount() == [5L, 0L, 1L]

    and: 'range buckets carry their bounds'
    aggs.buckets('bands')*.key() == ['small', '50000.0-*']
    aggs.buckets('bands')[0].from() == null
    aggs.buckets('bands')[0].to() == 50000d
    aggs.buckets('bands')[1].from() == 50000d
    aggs.buckets('bands')[1].to() == null
    aggs.buckets('quarters')[0].fromAsString() == '1704067200000'
    aggs.buckets('quarters')[0].toAsString() == '1711929600000'
    aggs.buckets('quarters')[0].from() == 1704067200000d

    and: 'keyed buckets take their key from the object key unless they carry one'
    aggs.buckets('keyed_bands')*.key() as Set == ['small', 'large'] as Set
    aggs.buckets('keyed_bands').find { it.key() == 'large' }.from() == 50000d
    aggs.buckets('keyed_months')*.key() == [1704067200000L, 1706745600000L, 1709251200000L, 1711929600000L,
                                            1714521600000L, 1717200000000L]
    aggs.buckets('keyed_months')*.keyString() == ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06']
    aggs.buckets('keyed_months')[0].sub().isEmpty()

    and: 'top-level metrics'
    aggs.metric('counted').asLong() == 6L
    aggs.metric('latest').valueAsString() == '2024-06-30T08:00:00.000Z'
  }

  def "lookups fail clearly on unknown names and kind mismatches"() {
    given:
    def aggs = sample()

    when:
    aggs.buckets('nope')

    then:
    def missing = thrown(NoSuchElementException)
    missing.message.startsWith("No aggregation named 'nope'; present: [bands, by_type, counted, days, flags,")

    when:
    aggs.buckets('counted')

    then:
    def metric = thrown(IllegalStateException)
    metric.message.contains('it is a metric')

    when:
    aggs.metric('by_type')

    then:
    def buckets = thrown(IllegalStateException)
    buckets.message.contains('it is a bucket aggregation')

    when:
    aggs.sumOtherDocCount('nope')

    then:
    thrown(NoSuchElementException)

    when:
    new AggregationResults.MetricValue(null, null).asLong()

    then:
    thrown(IllegalStateException)
  }

  def "results come from a search result or a raw map, and compare by content"() {
    given:
    Map<String, Object> raw = [n: [value: 2]]

    expect:
    AggregationResults.of((Map) null) == AggregationResults.empty()
    AggregationResults.of([:]).isEmpty()
    AggregationResults.empty().names().isEmpty()
    AggregationResults.of(new SearchResult(1, true, [], null, raw, 0)) == AggregationResults.of(raw)
    AggregationResults.of(raw).hashCode() == AggregationResults.of([n: [value: 2]]).hashCode()
    AggregationResults.of(raw) != AggregationResults.empty()
    AggregationResults.of(raw).toString() == 'AggregationResults{n={value=2}}'
    AggregationResults.of(raw).metric('n').asLong() == 2L
    AggregationResults.of([n: [sum_other_doc_count: 'x', buckets: []]]).sumOtherDocCount('n') == 0
    AggregationResults.of([n: [buckets: [[key: (short) 1, doc_count: 2], [key: new BigInteger('3')],
                                         [key: new BigDecimal('1.5')], [key: true]]]]).buckets('n')*.key() ==
        [1L, 3L, 1.5d, true]
    AggregationResults.of([n: [buckets: [[key: 'a']]]]).buckets('n')[0].docCount() == 0
    AggregationResults.of([doc_count: 3, x__nested: [doc_count: 1]]).names() == ['x'] as Set
    !AggregationResults.of([x__nested: [doc_count: 1]]).has('x')
    !AggregationResults.of([x__nested: 'no map']).has('x')
  }

  def "names() strips only a wrapper suffix of the outermost key"() {
    expect:
    AggregationResults.of(raw).names() == names as Set

    where:
    raw                                                  || names
    [x__nested2: [doc_count: 1, x: [value: 1]]]          || ['x']
    [x__reverse: [doc_count: 1, x: [value: 1]]]          || ['x']
    [a_b: [value: 1], c__nested: [doc_count: 0]]         || ['a_b', 'c']
    [a__b: [value: 1]]                                   || ['a__b']
    [a__nestedx: [value: 1]]                             || ['a__nestedx']
    [x__nested__reverse: [doc_count: 1]]                 || ['x__nested']
  }

  def "a wrapper is only unwrapped for the name it carries"() {
    given:
    def aggs = AggregationResults.of([x__nested: [doc_count: 1, x: [value: 1]], xy__nested: [doc_count: 1, xy: [value: 2]]])

    expect:
    aggs.metric('x').asLong() == 1L
    aggs.metric('xy').asLong() == 2L
    !aggs.has('x__')
  }

  def "the raw map is unmodifiable"() {
    when:
    AggregationResults.of([n: [value: 2]]).raw().put('m', 1)

    then:
    thrown(UnsupportedOperationException)
  }

  // ---- filter, filters, composite and three levels ------------------------------------------

  /**
   * The aggregations of one search over the testkit sample documents (golden-equivalent criteria:
   * filters 'kinds' with other bucket 'rest' and a max, filter 'filings' with a nested condition,
   * filter 'plaintiffs' over the nested parties, terms 'by_type' > nested terms 'roles' >
   * reverse-nested cardinality 'firms', a nested composite 'roles_by_name' and filters
   * 'none_matched'). Recorded from Elasticsearch 8.19.23 and 9.5.5 and OpenSearch 2.19.7 and
   * 3.9.0: their aggregations were identical once keys were sorted, and the keyed buckets came in
   * the same order on all four; the file keeps Elasticsearch 9.5.5's key order.
   */
  static AggregationResults recorded() {
    AggregationResults.of(Json.parseObject(
        new File('src/test/resources/responses/aggregations-filters-composite.json').text))
  }

  /** The three pages of composite 'pairs' (size 4), recorded like {@link #recorded()}. */
  static Map<String, AggregationResults> pages() {
    Json.parseObject(new File('src/test/resources/responses/composite-pages.json').text)
        .collectEntries { name, aggs -> [(name): AggregationResults.of((Map) aggs)] }
  }

  def "filters buckets are keyed, and come sorted by key"() {
    when:
    def kinds = recorded().buckets('kinds')

    then:
    kinds*.key() == ['acme', 'filings', 'rest']
    kinds*.docCount() == [2L, 3L, 2L]
    kinds*.sub()*.metric('latest')*.valueAsString() ==
        ['2024-02-20T14:00:00.000Z', '2024-06-30T08:00:00.000Z', '2024-04-05T16:45:00.000Z']
    recorded().buckets('none_matched')[0].sub().buckets('tags')*.key() == ['civil', 'urgent']
  }

  def "a filter is a single bucket, also over nested objects behind the wrapper"() {
    when:
    def aggs = recorded()

    then:
    aggs.bucket('filings').key() == null
    aggs.bucket('filings').docCount() == 1L
    aggs.bucket('filings').sub().metric('total').value() == 120_000d

    and: 'three plaintiff parties, in three documents'
    aggs.has('plaintiffs')
    aggs.raw().containsKey('plaintiffs__nested')
    aggs.bucket('plaintiffs').docCount() == 3L
    aggs.bucket('plaintiffs').sub().buckets('names')*.key() == ['Acme Corp', 'Initech LLC', 'Jane Doe']
    aggs.bucket('plaintiffs').sub().metric('docs').asLong() == 3L
  }

  def "three levels: type > nested roles > reverse-nested firm count, unwrapped at every level"() {
    when:
    def byType = recorded().buckets('by_type')

    then:
    byType*.key() == ['FILING', 'MOTION', 'NOTICE', 'ORDER']
    byType[0].sub().names() == ['roles'] as Set
    byType[0].sub().buckets('roles')*.key() == ['DEFENDANT', 'PLAINTIFF']
    byType[0].sub().buckets('roles')*.docCount() == [2L, 1L]
    byType[0].sub().buckets('roles')*.sub()*.metric('firms')*.asLong() == [2L, 1L]
    byType[2].sub().buckets('roles')*.key() == ['COUNSEL']
  }

  def "composite buckets have map keys (sorted by source name, as the adapters parse them), and the after key resumes"() {
    when:
    def aggs = recorded()
    def buckets = aggs.buckets('roles_by_name')

    then: 'a nested composite, unwrapped'
    buckets.size() == 7
    buckets[0].key() == [role: 'COUNSEL', name: 'Saul Goodman']
    buckets[0].compositeKey().keySet() as List == ['name', 'role']
    buckets[0].keyString() == '{name=Saul Goodman, role=COUNSEL}'
    buckets[2].docCount() == 2L
    buckets[2].sub().metric('docs').asLong() == 2L
    aggs.afterKey('roles_by_name') == Optional.of([role: 'PLAINTIFF', name: 'Jane Doe'])

    when: 'the pages of a composite with a missing bucket'
    def pages = pages()

    then:
    pages.page1.buckets('pairs')*.key() == [
        [firm: 'firm-1', month: '2024-01', relevance: null], [firm: 'firm-1', month: '2024-05', relevance: null],
        [firm: 'firm-2', month: '2024-02', relevance: null], [firm: 'firm-2', month: '2024-03', relevance: null]]
    pages.page1.afterKey('pairs').get() == [firm: 'firm-2', month: '2024-03', relevance: null]
    pages.page2.buckets('pairs')*.key()*.relevance == [null, 0.75d]
    pages.page2.buckets('pairs')[1].sub().metric('total').asLong() == 2_500_000L
    pages.page2.afterKey('pairs').get() == [firm: 'firm-3', month: '2024-06', relevance: 0.75d]

    and: 'the last page has neither buckets nor an after key'
    pages.page3.buckets('pairs').isEmpty()
    when: 'a page without buckets cannot be told from an empty histogram, so it has no after key to read'
    pages.page3.afterKey('pairs')

    then:
    thrown(IllegalStateException)
  }

  def "composite keys are unmodifiable and normalized like other keys"() {
    given:
    def bucket = AggregationResults.of([c: [buckets: [[key: [a: 1, b: 2.5d, c: true, d: null], doc_count: 1]]]])
        .buckets('c')[0]

    when:
    bucket.compositeKey().put('x', 1)

    then:
    thrown(UnsupportedOperationException)
    bucket.compositeKey() == [a: 1L, b: 2.5d, c: true, d: null]
    bucket.compositeKey().a instanceof Long
  }

  def "kind mismatches for single-bucket and composite aggregations"() {
    given:
    def aggs = recorded()

    when:
    aggs.buckets('filings')

    then:
    def single = thrown(IllegalStateException)
    single.message == "Aggregation 'filings' has no buckets; it is a single-bucket aggregation, read it with bucket(\"filings\")"

    when:
    aggs.metric('filings')

    then:
    def singleMetric = thrown(IllegalStateException)
    singleMetric.message.contains('it is a single-bucket aggregation, read it with bucket("filings")')

    when:
    aggs.bucket('kinds')

    then:
    def multi = thrown(IllegalStateException)
    multi.message == "Aggregation 'kinds' has several buckets; read them with buckets(\"kinds\")"

    when:
    sample().bucket('counted')

    then:
    def metric = thrown(IllegalStateException)
    metric.message.contains('it is a metric, read it with metric("counted")')

    when:
    aggs.afterKey('by_type')

    then:
    def notComposite = thrown(IllegalStateException)
    notComposite.message == "Aggregation 'by_type' is not a composite aggregation with buckets; only composite aggregations have an after key, and a composite page without buckets (the last one) has none"

    when:
    AggregationResults.of([h: [buckets: []]]).afterKey('h')

    then:
    thrown(IllegalStateException)

    expect: 'a filter bucket has no key string'
    aggs.bucket('filings').keyString() == null

    when:
    sample().afterKey('bands')

    then:
    thrown(IllegalStateException)

    when:
    aggs.afterKey('filings')

    then:
    thrown(IllegalStateException)

    when:
    aggs.bucket('nope')

    then:
    def missing = thrown(NoSuchElementException)
    missing.message.contains("No aggregation named 'nope'; present: [")

    when:
    buckets(aggs)[0].compositeKey()

    then:
    thrown(IllegalStateException)
  }

  private static List<AggregationResults.Bucket> buckets(AggregationResults aggs) {
    aggs.buckets('by_type')
  }
}
