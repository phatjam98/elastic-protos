package com.phatjam98.elasticprotos.query

import com.phatjam98.elasticprotos.proto.Aggregation
import com.phatjam98.elasticprotos.proto.CalendarInterval
import com.phatjam98.elasticprotos.proto.Highlight
import com.phatjam98.elasticprotos.proto.HighlightField
import com.phatjam98.elasticprotos.proto.MetricType
import com.phatjam98.elasticprotos.proto.RangeBucket
import com.phatjam98.elasticprotos.proto.TermsAggregation
import spock.lang.Specification
import spock.lang.Unroll

class AggsSpec extends Specification {

  def "factories set the name, the field and the kind"() {
    expect:
    Aggs.terms('t', 'type').build() ==
        Aggregation.newBuilder().setName('t').setField('type').setTerms(TermsAggregation.defaultInstance).build()
    Aggs.dateHistogram('d', 'filed_at', CalendarInterval.MONTH).build().dateHistogram.calendarInterval ==
        CalendarInterval.MONTH
    Aggs.dateHistogram('d', 'filed_at', '90m').build().dateHistogram.fixedInterval == '90m'
    Aggs.histogram('h', 'size', 2.5).build().histogram.interval == 2.5d
    Aggs.range('r', 'size', Aggs.bucket('a', null, 1d), Aggs.bucket(null, 1d, null)).build().range.rangesList ==
        [RangeBucket.newBuilder().setKey('a').setTo(1).build(), RangeBucket.newBuilder().setFrom(1).build()]
    Aggs.bucket(null, null, null) == RangeBucket.defaultInstance
    !Aggs.bucket(null, null, null).hasFrom()
  }

  @Unroll
  def "#factory is the #type metric"() {
    expect:
    Aggs."${factory}"('m', 'f').build() ==
        Aggregation.newBuilder().setName('m').setField('f').setMetric(type).build()

    where:
    factory       | type
    'min'         | MetricType.MIN
    'max'         | MetricType.MAX
    'avg'         | MetricType.AVG
    'sum'         | MetricType.SUM
    'count'       | MetricType.VALUE_COUNT
    'cardinality' | MetricType.CARDINALITY
  }

  def "sub adds sub-aggregations from messages or builders"() {
    expect:
    Aggs.sub(Aggs.terms('t', 'type'), Aggs.max('m', 'size').build(), Aggs.min('n', 'size').build())
        .build().subAggregationList*.name == ['m', 'n']
    Aggs.sub(Aggs.terms('t', 'type'), Aggs.max('m', 'size'), Aggs.min('n', 'size'))
        .build().subAggregationList*.name == ['m', 'n']
  }

  def "null arguments are refused"() {
    when:
    call()

    then:
    thrown(NullPointerException)

    where:
    call << [
        { Aggs.terms(null, 'f') },
        { Aggs.terms('n', null) },
        { Aggs.dateHistogram('n', 'f', (CalendarInterval) null) },
        { Aggs.dateHistogram('n', 'f', (String) null) },
        { Aggs.metric('n', 'f', null) },
        { Aggs.range('n', 'f', (RangeBucket) null) },
        { Aggs.sub(Aggs.terms('t', 'f'), (Aggregation) null) },
        { Aggs.sub(Aggs.terms('t', 'f'), (Aggregation.Builder) null) },
        { Criteria.builder().aggregate((Aggregation) null) },
        { Criteria.builder().aggregate((Aggregation.Builder) null) },
        { Criteria.builder().highlight((String) null) },
    ]
  }

  def "the criteria builder takes aggregations, aggregations-only paging and highlighting"() {
    when:
    def criteria = Criteria.builder()
        .aggregate(Aggs.terms('t', 'type'))
        .aggregate(Aggs.max('m', 'size').build())
        .aggregationsOnly()
        .highlight('title')
        .highlight { it.setPreTag('<b>').addFields(HighlightField.newBuilder().setField('body')) }
        .build()

    then:
    criteria.aggregationList*.name == ['t', 'm']
    criteria.pagination.size == -1
    criteria.highlight == Highlight.newBuilder().setPreTag('<b>')
        .addFields(HighlightField.newBuilder().setField('title'))
        .addFields(HighlightField.newBuilder().setField('body')).build()

    expect: 'highlight() without fields asks for the default fields'
    Criteria.builder().highlight().build().hasHighlight()
    Criteria.builder().highlight().build().highlight.fieldsCount == 0
    !Criteria.builder().build().hasHighlight()
  }

  def "aggregationsOnly compiles to size 0"() {
    expect:
    QueryCompiler.withDefaults().compile(Criteria.builder().aggregationsOnly().build(),
        com.phatjam98.elasticprotos.mapping.ProtoMapper.withDefaults().map(
            com.phatjam98.elasticprotos.proto.example.Document)).body().size == 0
  }
}
