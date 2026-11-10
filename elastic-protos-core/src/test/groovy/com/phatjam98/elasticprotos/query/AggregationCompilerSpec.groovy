package com.phatjam98.elasticprotos.query

import static com.phatjam98.elasticprotos.query.Q.prim

import com.google.protobuf.Timestamp
import com.phatjam98.elasticprotos.mapping.IndexDefinition
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.Aggregation
import com.phatjam98.elasticprotos.proto.CalendarInterval
import com.phatjam98.elasticprotos.proto.DateBounds
import com.phatjam98.elasticprotos.proto.DateHistogramAggregation
import com.phatjam98.elasticprotos.proto.HistogramAggregation
import com.phatjam98.elasticprotos.proto.MetricAggregation
import com.phatjam98.elasticprotos.proto.MetricType
import com.phatjam98.elasticprotos.proto.RangeBucket
import com.phatjam98.elasticprotos.proto.SortingOrder
import com.phatjam98.elasticprotos.proto.TermsAggregation
import com.phatjam98.elasticprotos.proto.TermsOrder
import com.phatjam98.elasticprotos.proto.TermsOrderBy
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.proto.example.TypedMaps
import com.phatjam98.elasticprotos.proto.test.Geo
import com.phatjam98.elasticprotos.proto.test.Library
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class AggregationCompilerSpec extends Specification {

  @Shared ProtoMapper mapper = ProtoMapper.withDefaults()
  @Shared IndexDefinition document = mapper.map(Document)
  @Shared IndexDefinition firm = mapper.map(Firm)
  @Shared IndexDefinition geo = mapper.map(Geo)
  @Shared IndexDefinition typedMaps = mapper.map(TypedMaps)
  @Shared IndexDefinition library = mapper.map(Library)
  @Shared QueryCompiler strict = QueryCompiler.withDefaults()
  @Shared QueryCompiler lenient = QueryCompiler.of(QueryCompilerConfig.standard().withLenient(true))

  static CompiledSearch compile(QueryCompiler compiler, IndexDefinition index, Object... aggs) {
    compiler.compile(Criteria.builder().aggregate(aggs.collect {
      it instanceof Aggregation.Builder ? it.build() : it } as Aggregation[]).build(), index)
  }

  static Map aggs(IndexDefinition index, Object... aggs) {
    compile(QueryCompiler.withDefaults(), index, aggs).body().aggs
  }

  static String error(IndexDefinition index, Object... aggs) {
    try {
      compile(QueryCompiler.withDefaults(), index, aggs)
    } catch (QueryCompileException e) {
      return e.message
    }
    throw new AssertionError('expected a QueryCompileException')
  }

  static Aggregation.Builder terms(String name, String field, @DelegatesTo(TermsAggregation.Builder) Closure options) {
    def builder = TermsAggregation.newBuilder()
    builder.with(options)
    Aggs.terms(name, field).setTerms(builder)
  }

  static Aggregation.Builder dateHistogram(String name, String field,
      @DelegatesTo(DateHistogramAggregation.Builder) Closure options) {
    def builder = DateHistogramAggregation.newBuilder()
    builder.with(options)
    Aggregation.newBuilder().setName(name).setField(field).setDateHistogram(builder)
  }

  static TermsOrder order(TermsOrderBy by, String metric = '', SortingOrder direction = SortingOrder.ORDER_UNDEFINED) {
    TermsOrder.newBuilder().setBy(by).setMetric(metric).setDirection(direction).build()
  }

  def "no aggregations leave aggs out of the body"() {
    expect:
    !strict.compile(Criteria.builder().build(), document).body().containsKey('aggs')
  }

  // ---- names and structure ---------------------------------------------------------------

  @Unroll
  def "the name '#name' is refused: #reason"() {
    expect:
    error(document, Aggs.terms(name, 'type')).contains(reason)

    where:
    name          | reason
    ''            | 'needs a name'
    'by__type'    | "must not contain '__'"
    'doc_count'   | 'is reserved'
    'buckets'     | 'is reserved'
    'key'         | 'is reserved'
    'value'       | 'is reserved'
    'meta'        | 'is reserved'
    'a[0]'        | "must not contain '['"
    'a]'          | "must not contain ']'"
    'a>b'         | "must not contain '>'"
    'a.b'         | "must not contain '.'"
    'total_'      | "must not end with '_'"
  }

  def "sibling names must be unique, but a sub-aggregation may reuse a name of another level"() {
    expect:
    error(document, Aggs.terms('a', 'type'), Aggs.max('a', 'size_bytes')).contains("'a' is used twice")
    error(document, Aggs.sub(Aggs.terms('a', 'type'), Aggs.max('m', 'size_bytes'), Aggs.min('m', 'size_bytes')))
        .contains("'m' is used twice")
    aggs(document, Aggs.sub(Aggs.terms('a', 'type'), Aggs.max('a', 'size_bytes'))).a.aggs.a ==
        [max: [field: 'size_bytes']]
  }

  def "an aggregation needs a kind and a field"() {
    expect:
    error(document, Aggregation.newBuilder().setName('x').setField('type')).contains('sets no kind')
    error(document, Aggregation.newBuilder().setName('x').setMetric(MetricAggregation.newBuilder().setType(MetricType.MAX))).contains('needs a field')
  }

  def "metrics take no sub-aggregations, and aggregations nest down to maxAggregationDepth levels"() {
    expect:
    error(document, Aggs.sub(Aggs.max('m', 'size_bytes'), Aggs.min('n', 'size_bytes')))
        .contains("Metric aggregation 'm' cannot have sub-aggregations")

    and: 'three levels by default'
    aggs(document, Aggs.sub(Aggs.terms('a', 'type'),
        Aggs.sub(Aggs.terms('b', 'tags'), Aggs.max('c', 'size_bytes')))).a.aggs.b.aggs.c == [max: [field: 'size_bytes']]
    error(document, Aggs.sub(Aggs.terms('a', 'type'), Aggs.sub(Aggs.terms('b', 'tags'),
        Aggs.sub(Aggs.terms('c', 'firm_id'), Aggs.max('d', 'size_bytes')))))
        .contains("Aggregation 'c' is on level 3 and cannot have sub-aggregations: at most 3 levels are allowed (QueryCompilerConfig.maxAggregationDepth)")
  }

  def "maxAggregationDepth is configurable, down to no sub-aggregations at all"() {
    given:
    def flat = QueryCompiler.of(QueryCompilerConfig.standard().withMaxAggregationDepth(1))
    def deep = QueryCompiler.of(QueryCompilerConfig.standard().withMaxAggregationDepth(4))
    def fourLevels = Aggs.sub(Aggs.terms('a', 'type'), Aggs.sub(Aggs.terms('b', 'tags'),
        Aggs.sub(Aggs.terms('c', 'firm_id'), Aggs.max('d', 'size_bytes'))))

    when:
    compile(flat, document, Aggs.sub(Aggs.terms('a', 'type'), Aggs.max('m', 'size_bytes')))

    then:
    def e = thrown(QueryCompileException)
    e.message.contains("Aggregation 'a' is on level 1 and cannot have sub-aggregations: at most 1 level is allowed")

    expect:
    compile(flat, document, Aggs.terms('a', 'type')).body().aggs.a == [terms: [field: 'type']]
    compile(deep, document, fourLevels).body().aggs.a.aggs.b.aggs.c.aggs.d == [max: [field: 'size_bytes']]
  }

  def "the wrappers, their names and BY_METRIC order paths work at every level"() {
    when: 'type > nested role (ordered by a reverse-nested metric) > reverse-nested cardinality'
    def a = aggs(document, Aggs.sub(Aggs.terms('by_type', 'type'),
        Aggs.sub(terms('roles', 'parties.role') { addOrder(order(TermsOrderBy.BY_METRIC, 'firms')) },
            Aggs.cardinality('firms', 'firm_id'), Aggs.count('emails', 'parties.email'))))

    then:
    a.by_type.aggs.keySet() == ['roles__nested'] as Set
    a.by_type.aggs.roles__nested == [nested: [path: 'parties'], aggs: [roles: [
        terms: [field: 'parties.role', order: ['firms__reverse>firms': 'desc']],
        aggs: [emails: [value_count: [field: 'parties.email']],
               firms__reverse: [reverse_nested: [:], aggs: [firms: [cardinality: [field: 'firm_id']]]]]]]]

    when: 'two nested levels below a nested bucket, then back up to the root'
    def lib = aggs(library, Aggs.sub(Aggs.terms('labels', 'shelves.label'),
        Aggs.sub(Aggs.terms('books', 'shelves.books.name'), Aggs.terms('cities', 'city'))))

    then:
    lib.labels__nested.aggs.labels.aggs.books__nested.aggs.books.aggs ==
        [cities__reverse: [reverse_nested: [:], aggs: [cities: [terms: [field: 'city']]]]]
  }

  def "maxAggregationBuckets refuses paths whose requested bucket counts multiply beyond it"() {
    given:
    def guarded = QueryCompiler.of(QueryCompilerConfig.standard().withMaxAggregationBuckets(1000))

    when: '100 x 10 (terms default) = 1000 passes'
    compile(guarded, document, Aggs.sub(terms('a', 'type') { size = 100 }, Aggs.terms('b', 'tags')))

    then:
    noExceptionThrown()

    when: '100 x 11 does not'
    compile(guarded, document, Aggs.sub(terms('a', 'type') { size = 100 }, terms('b', 'tags') { size = 11 }))

    then:
    def e = thrown(QueryCompileException)
    e.message.contains("Aggregations 'a>b' ask for up to 1100 buckets (the product of their sizes), more than the maximum of 1000 (QueryCompilerConfig.maxAggregationBuckets)")

    when: 'ranges and filters count their buckets, histograms, filter and nested wrappers count one'
    compile(QueryCompiler.of(guarded.config().withMaxAggregationDepth(4)), document, Aggs.sub(Aggs.range('r', 'size_bytes', Aggs.bucket(null, 1d, 2d), Aggs.bucket(null, 2d, 3d)),
        Aggs.sub(Aggs.histogram('h', 'size_bytes', 10), Aggs.sub(Aggs.filters('f',
            Aggs.keyed('x', Criteria.where('type').equalTo('A')), Aggs.keyed('y', Criteria.where('type').equalTo('B')))
            .tap { filtersBuilder.setOtherBucketKey('rest') }, terms('t', 'tags') { size = 200 }))))

    then: '2 x 1 x 3 x 200 = 1200'
    def e2 = thrown(QueryCompileException)
    e2.message.contains("'r>h>f>t' ask for up to 1200 buckets")

    when: 'a nested path is reported without its wrappers'
    compile(guarded, document, Aggs.sub(terms('a', 'type') { size = 100 }, terms('r', 'parties.role') { size = 11 }))

    then:
    def e3 = thrown(QueryCompileException)
    e3.message.contains("Aggregations 'a>r' ask for up to 1100 buckets")

    when: 'validation comes first, and aggregations lenient mode skipped do not count'
    compile(guarded, document, Aggs.sub(terms('a__x', 'type') { size = 100 }, terms('b', 'tags') { size = 11 }))

    then:
    def e4 = thrown(QueryCompileException)
    e4.message.contains("must not contain '__'")

    expect:
    compile(QueryCompiler.of(guarded.config().withLenient(true)), document,
        Aggs.sub(terms('a', 'type') { size = 100 }, terms('b', 'nope') { size = 11 })).warnings() ==
        ["Skipped aggregation 'b' on unknown field 'nope'"]

    and: 'the guard is off by default'
    compile(strict, document, Aggs.sub(terms('a', 'type') { size = 1000 }, terms('b', 'tags') { size = 1000 }))
        .body().aggs.a.aggs.b.terms.size == 1000
  }

  // ---- unknown fields ----------------------------------------------------------------------

  def "an unknown field throws in strict mode and is skipped with its sub-aggregations in lenient mode"() {
    expect:
    error(document, Aggs.terms('x', 'nope')).contains("Unknown field 'nope' in aggregation 'x'; index documents does not map it")

    when:
    def compiled = compile(lenient, document, Aggs.sub(Aggs.terms('x', 'nope'), Aggs.max('m', 'size_bytes')),
        Aggs.sub(Aggs.terms('y', 'type'), Aggs.max('m', 'nope2')))

    then:
    compiled.body().aggs == [y: [terms: [field: 'type']]]
    compiled.warnings() == ["Skipped aggregation 'x' on unknown field 'nope'",
                            "Skipped aggregation 'm' on unknown field 'nope2'"]

    when: 'every aggregation skipped'
    def none = compile(lenient, document, Aggs.max('m', 'nope'))

    then:
    !none.body().containsKey('aggs')
  }

  def "an unknown path below a typed map names the value type"() {
    expect:
    error(typedMaps, Aggs.terms('x', 'children.k.nope')).contains("the values of map 'children'")
  }

  // ---- terms -------------------------------------------------------------------------------

  def "terms use the keyword sub-field of text, keys of flattened fields and typed map keys"() {
    expect:
    aggs(firm, Aggs.terms('n', 'name')).n == [terms: [field: 'name.keyword']]
    aggs(firm, Aggs.terms('t', 'labels.tier')).t == [terms: [field: 'labels.tier']]
    aggs(typedMaps, Aggs.terms('v', 'counters.views')).v == [terms: [field: 'counters.views']]
    aggs(typedMaps, Aggs.terms('l', 'levels.k')).l == [terms: [field: 'levels.k']]
    aggs(document, Aggs.terms('d', 'filed_at')).d == [terms: [field: 'filed_at']]
    aggs(document, Aggs.terms('s', 'size_bytes')).s == [terms: [field: 'size_bytes']]
  }

  def "terms on text without a keyword sub-field points at keyword_subfield"() {
    expect:
    error(document, Aggs.terms('b', 'body')).contains('keyword_subfield')
    error(document, Aggs.cardinality('b', 'body')).contains('keyword_subfield')
  }

  @Unroll
  def "terms are refused on #field mapped as #kind"() {
    expect:
    error(index, Aggs.terms('x', field)).contains(message)

    where:
    index    | field                   | kind         | message
    document | 'parties'               | 'NESTED'     | 'mapped as NESTED'
    document | 'checksum'              | 'BINARY'     | 'mapped as BINARY'
    document | 'metadata'              | 'FLATTENED'  | "aggregate one of its keys, such as 'metadata.<key>'"
    firm     | 'headquarters'          | 'OBJECT'     | 'mapped as OBJECT'
    firm     | 'headquarters.location' | 'GEO_POINT'  | 'mapped as GEO_POINT'
    geo      | 'shape'                 | 'GEO_SHAPE'  | 'mapped as GEO_SHAPE'
  }

  def "terms options: size, min_doc_count, missing typed for the field, and order"() {
    expect:
    aggs(document, terms('x', 'size_bytes') { size = 3; minDocCount = 0; missing = prim('7') }).x ==
        [terms: [field: 'size_bytes', size: 3, min_doc_count: 0, missing: 7L]]
    aggs(document, terms('x', 'type') { missing = prim('NONE') }).x.terms.missing == 'NONE'
    aggs(document, terms('x', 'type') { addOrder(order(TermsOrderBy.BY_COUNT, '', SortingOrder.ASC)) }).x.terms.order ==
        [_count: 'asc']
    aggs(document, terms('x', 'type') { addOrder(order(TermsOrderBy.BY_COUNT)) }).x.terms.order == [_count: 'desc']
    aggs(document, terms('x', 'type') { addOrder(order(TermsOrderBy.BY_KEY)) }).x.terms.order == [_key: 'asc']
    aggs(document, terms('x', 'type') { addOrder(order(TermsOrderBy.ORDER_BY_DEFAULT, '', SortingOrder.ASC)) }).x.terms.order ==
        [_count: 'asc']
    !aggs(document, terms('x', 'type') { addOrder(order(TermsOrderBy.ORDER_BY_DEFAULT)) }).x.terms.containsKey('order')
  }

  def "terms options are validated"() {
    expect:
    error(document, terms('x', 'type') { size = 1001 }).contains('more than the maximum of 1000 (QueryCompilerConfig.maxTermsSize)')
    error(document, terms('x', 'type') { size = -1 }).contains('negative size')
    error(document, terms('x', 'type') { minDocCount = -1 }).contains('negative min_doc_count')
    error(document, terms('x', 'size_bytes') { missing = prim('abc') }).contains('cannot take string_value "abc"')
    error(document, terms('x', 'type') { addOrder(order(TermsOrderBy.BY_KEY, 'm')) }).contains('set by = BY_METRIC')
    error(document, terms('x', 'type') { addOrder(TermsOrder.newBuilder().setByValue(42).build()) }).contains('unknown order')
    error(document, terms('x', 'type') { addOrder(TermsOrder.newBuilder().setBy(TermsOrderBy.BY_KEY).setDirectionValue(42).build()) })
        .contains('unknown order direction')
  }

  def "maxTermsSize is configurable"() {
    given:
    def small = QueryCompiler.of(QueryCompilerConfig.standard().withMaxTermsSize(5))

    when:
    compile(small, document, terms('x', 'type') { size = 6 })

    then:
    def e = thrown(QueryCompileException)
    e.message.contains('maximum of 5')
    e.field == 'type'

    expect:
    compile(small, document, terms('x', 'type') { size = 5 }).body().aggs.x.terms.size == 5
  }

  def "several orders compile to a compound order, the first deciding"() {
    expect:
    aggs(document, Aggs.sub(terms('x', 'type') {
      addOrder(order(TermsOrderBy.BY_METRIC, 'm'))
      addOrder(order(TermsOrderBy.BY_KEY))
    }, Aggs.sum('m', 'size_bytes'))).x.terms.order == [[m: 'desc'], [_key: 'asc']]
    !aggs(document, terms('x', 'type') {
      addOrder(order(TermsOrderBy.ORDER_BY_DEFAULT))
      addOrder(order(TermsOrderBy.ORDER_BY_DEFAULT))
    }).x.terms.containsKey('order')
  }

  def "ordering by a metric names a metric sub-aggregation"() {
    expect:
    aggs(document, Aggs.sub(terms('x', 'type') { addOrder(order(TermsOrderBy.BY_METRIC, 'm')) },
        Aggs.sum('m', 'size_bytes'))).x.terms.order == [m: 'desc']
    aggs(document, Aggs.sub(terms('x', 'type') { addOrder(order(TermsOrderBy.BY_METRIC, 'm', SortingOrder.ASC)) },
        Aggs.sum('m', 'size_bytes'))).x.terms.order == [m: 'asc']

    and:
    error(document, terms('x', 'type') { addOrder(order(TermsOrderBy.BY_METRIC)) }).contains('names no metric')
    error(document, Aggs.sub(terms('x', 'type') { addOrder(order(TermsOrderBy.BY_METRIC, 'other')) },
        Aggs.sum('m', 'size_bytes'))).contains("orders by 'other', which is not one of its sub-aggregations")
    error(document, Aggs.sub(terms('x', 'type') { addOrder(order(TermsOrderBy.BY_METRIC, 'b')) },
        Aggs.terms('b', 'tags'))).contains("which is a bucket aggregation")
  }

  def "in lenient mode an order metric that was skipped leaves the default order"() {
    when:
    def compiled = compile(lenient, document, Aggs.sub(terms('x', 'type') { addOrder(order(TermsOrderBy.BY_METRIC, 'm')) },
        Aggs.sum('m', 'nope')))

    then:
    compiled.body().aggs == [x: [terms: [field: 'type']]]
    compiled.warnings() == ["Skipped aggregation 'm' on unknown field 'nope'",
                            "Terms aggregation 'x' keeps the default order: its order metric 'm' was skipped"]
  }

  // ---- histograms ----------------------------------------------------------------------------

  @Unroll
  def "calendar interval #interval is #expected"() {
    expect:
    aggs(document, Aggs.dateHistogram('h', 'filed_at', interval)).h.date_histogram.calendar_interval == expected

    where:
    interval                  | expected
    CalendarInterval.MINUTE   | '1m'
    CalendarInterval.HOUR     | '1h'
    CalendarInterval.DAY      | '1d'
    CalendarInterval.WEEK     | '1w'
    CalendarInterval.MONTH    | '1M'
    CalendarInterval.QUARTER  | '1q'
    CalendarInterval.YEAR     | '1y'
  }

  @Unroll
  def "fixed interval '#interval' is accepted: #ok"() {
    when:
    def message = ok ? null : error(document, Aggs.dateHistogram('h', 'filed_at', interval))

    then:
    ok ? aggs(document, Aggs.dateHistogram('h', 'filed_at', interval)).h.date_histogram.fixed_interval == interval
        : message.contains("fixed_interval '${interval}'")

    where:
    interval | ok
    '90m'    | true
    '500ms'  | true
    '1d'     | true
    '30s'    | true
    '2h'     | true
    '0m'     | false
    '1M'     | false
    '1w'     | false
    '1.5h'   | false
    'h'      | false
    ''       | false
  }

  def "date histogram options and checks"() {
    expect:
    aggs(document, dateHistogram('h', 'filed_at') {
      calendarInterval = CalendarInterval.DAY; timeZone = '-05:00'; format = 'yyyy-MM-dd'; minDocCount = 2
      extendedBounds = DateBounds.newBuilder().setMin(Timestamp.newBuilder().setSeconds(10)).build()
    }).h.date_histogram == [field: 'filed_at', calendar_interval: '1d', time_zone: '-05:00', format: 'yyyy-MM-dd',
                            min_doc_count: 2, extended_bounds: [min: 10_000L]]
    !aggs(document, dateHistogram('h', 'filed_at') {
      calendarInterval = CalendarInterval.DAY; extendedBounds = DateBounds.defaultInstance
    }).h.date_histogram.containsKey('extended_bounds')

    and:
    error(document, dateHistogram('h', 'filed_at') {}).contains('needs calendar_interval or fixed_interval')
    error(document, dateHistogram('h', 'filed_at') { calendarIntervalValue = 0 }).contains('no valid calendar_interval')
    error(document, dateHistogram('h', 'filed_at') { calendarInterval = CalendarInterval.DAY; timeZone = 'Mars/Olympus' })
        .contains("invalid time_zone 'Mars/Olympus'")
    error(document, dateHistogram('h', 'filed_at') { calendarInterval = CalendarInterval.DAY; minDocCount = -2 })
        .contains('negative min_doc_count')
    error(document, dateHistogram('h', 'filed_at') {
      calendarInterval = CalendarInterval.DAY
      extendedBounds = DateBounds.newBuilder().setMin(Timestamp.newBuilder().setSeconds(20))
          .setMax(Timestamp.newBuilder().setSeconds(10)).build()
    }).contains('min after max')
    error(document, dateHistogram('h', 'filed_at') {
      calendarInterval = CalendarInterval.DAY
      extendedBounds = DateBounds.newBuilder().setMin(Timestamp.newBuilder().setSeconds(Long.MAX_VALUE)).build()
    }).contains('invalid extended_bounds timestamp')
    error(document, Aggs.dateHistogram('h', 'size_bytes', CalendarInterval.DAY)).contains('needs a date field')
    error(firm, Aggs.dateHistogram('h', 'labels.tier', CalendarInterval.DAY)).contains('a key of a flattened field')
  }

  def "histogram options and checks"() {
    expect:
    aggs(document, Aggs.histogram('h', 'size_bytes', 2.5)).h == [histogram: [field: 'size_bytes', interval: 2.5d]]
    aggs(document, Aggs.histogram('h', 'size_bytes', 10).setHistogram(
        HistogramAggregation.newBuilder().setInterval(10).setOffset(2.5).setMinDocCount(1))).h.histogram ==
        [field: 'size_bytes', interval: 10L, offset: 2.5d, min_doc_count: 1]

    and:
    error(document, Aggs.histogram('h', 'size_bytes', interval)).contains('positive, finite interval')
    error(document, Aggs.histogram('h', 'filed_at', 1)).contains('needs a numeric field')
    error(document, Aggs.histogram('h', 'size_bytes', 1).setHistogram(
        HistogramAggregation.newBuilder().setInterval(1).setOffset(Double.NaN))).contains('finite offset')
    error(firm, Aggs.histogram('h', 'labels.tier', 1)).contains('a key of a flattened field')

    where:
    interval << [0d, -1d, Double.NaN, Double.POSITIVE_INFINITY]
  }

  // ---- ranges --------------------------------------------------------------------------------

  def "date range buckets take Timestamp bounds as epoch millis, numeric fields refuse them"() {
    expect:
    aggs(document, Aggs.range('r', 'filed_at', Aggs.bucket('q1', java.time.Instant.parse('2024-01-01T00:00:00Z'),
        java.time.Instant.parse('2024-04-01T00:00:00.250Z')), Aggs.bucket(null, java.time.Instant.parse('2024-04-01T00:00:00Z'),
        (java.time.Instant) null))).r.date_range.ranges == [[key: 'q1', from: 1704067200000L, to: 1711929600250L],
                                                            [from: 1711929600000L]]
    aggs(document, Aggs.range('r', 'filed_at', RangeBucket.newBuilder().setFrom(5).setToTime(Timestamp.newBuilder().setSeconds(10)).build()))
        .r.date_range.ranges == [[from: 5L, to: 10_000L]]

    and:
    error(document, Aggs.range('r', 'size_bytes', Aggs.bucket('k', java.time.Instant.EPOCH, (java.time.Instant) null)))
        .contains("has a from_time or to_time bound, but 'size_bytes' is mapped as LONG")
    error(document, Aggs.range('r', 'filed_at', RangeBucket.newBuilder().setToTime(Timestamp.newBuilder().setSeconds(Long.MAX_VALUE)).build()))
        .contains('invalid timestamp bound')
    error(document, Aggs.range('r', 'filed_at', Aggs.bucket(null, java.time.Instant.parse('2024-02-01T00:00:00Z'),
        java.time.Instant.parse('2024-01-01T00:00:00Z')))).contains('empty bucket')
  }

  def "range buckets are [from, to), and natural-side infinities mean unset"() {
    expect:
    aggs(document, Aggs.range('r', 'size_bytes', Aggs.bucket('a', Double.NEGATIVE_INFINITY, 10.5d),
        Aggs.bucket(null, 10.5d, Double.POSITIVE_INFINITY))).r ==
        [range: [field: 'size_bytes', ranges: [[key: 'a', to: 10.5d], [from: 10.5d]]]]
    aggs(document, Aggs.range('r', 'filed_at', Aggs.bucket(null, 1000d, 2000d))).r ==
        [date_range: [field: 'filed_at', format: 'epoch_millis', ranges: [[from: 1000L, to: 2000L]]]]
  }

  @Unroll
  def "a range aggregation is refused: #reason"() {
    expect:
    error(document, Aggs.range('r', field, buckets as RangeBucket[])).contains(reason)

    where:
    field        | buckets                                                  || reason
    'size_bytes' | []                                                       || 'needs at least one bucket'
    'size_bytes' | [Aggs.bucket('k', (Double) null, (Double) null)]                           || "bucket 'k' without from or to"
    'size_bytes' | [Aggs.bucket(null, 5d, 5d)]                              || 'empty bucket'
    'size_bytes' | [Aggs.bucket('k', 6d, 5d)]                               || "empty bucket 'k'"
    'size_bytes' | [Aggs.bucket(null, Double.POSITIVE_INFINITY, null)]      || 'empty bucket'
    'size_bytes' | [Aggs.bucket(null, null, Double.NEGATIVE_INFINITY)]      || 'empty bucket'
    'size_bytes' | [Aggs.bucket(null, Double.NaN, 3d)]                      || 'NaN bound'
    'size_bytes' | [Aggs.bucket(null, 1d, Double.NaN)]                      || 'NaN bound'
    'type'       | [Aggs.bucket(null, 1d, 2d)]                              || 'needs a numeric or date field'
  }

  def "a range on a flattened key is refused"() {
    expect:
    error(firm, Aggs.range('r', 'labels.tier', Aggs.bucket(null, 1d, 2d))).contains('a key of a flattened field')
  }

  // ---- metrics -------------------------------------------------------------------------------

  def "metrics check the field kind"() {
    expect:
    aggs(document, Aggs.min('a', 'filed_at')).a == [min: [field: 'filed_at']]
    aggs(document, Aggs.count('a', 'title')).a == [value_count: [field: 'title.keyword']]
    aggs(firm, Aggs.cardinality('a', 'labels.tier')).a == [cardinality: [field: 'labels.tier']]
    aggs(document, Aggs.metric('a', 'size_bytes', MetricType.AVG)).a == [avg: [field: 'size_bytes']]

    and:
    error(document, Aggs.max('a', 'type')).contains('needs a numeric or date field')
    error(document, Aggs.avg('a', 'filed_at')).contains('needs a numeric field')
    error(document, Aggs.sum('a', 'type')).contains('needs a numeric field')
    error(document, Aggs.count('a', 'parties')).contains('mapped as NESTED')
    error(document, Aggs.metric('a', 'size_bytes', MetricType.METRIC_UNSPECIFIED)).contains('no valid metric')
    error(document, Aggregation.newBuilder().setName('a').setField('size_bytes').setMetric(MetricAggregation.newBuilder().setTypeValue(99))).contains('no valid metric')
  }

  // ---- nested --------------------------------------------------------------------------------

  def "nested fields are wrapped once per level and stepped out of with reverse_nested"() {
    expect:
    aggs(document, Aggs.count('m', 'parties.role')) ==
        [m__nested: [nested: [path: 'parties'], aggs: [m: [value_count: [field: 'parties.role']]]]]
    aggs(firm, Aggs.sub(Aggs.terms('o', 'offices.address.city'), Aggs.max('h', 'offices.headcount'),
        Aggs.terms('s', 'status'))).o__nested.aggs.o.aggs ==
        [h: [max: [field: 'offices.headcount']],
         s__reverse: [reverse_nested: [:], aggs: [s: [terms: [field: 'status']]]]]
  }

  def "nested paths below a typed map are refused"() {
    expect:
    error(typedMaps, Aggs.terms('s', 'children.k.items.sku'))
        .contains("inside nested path 'children.k.items' of a typed map")
  }
}
