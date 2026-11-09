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
}
