package com.phatjam98.elasticprotos.query

import com.phatjam98.elasticprotos.mapping.IndexDefinition
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.Aggregation
import com.phatjam98.elasticprotos.proto.CalendarInterval
import com.phatjam98.elasticprotos.proto.CompositeSource
import com.phatjam98.elasticprotos.proto.FilterAggregation
import com.phatjam98.elasticprotos.proto.KeyedFilter
import com.phatjam98.elasticprotos.proto.PrimitiveValue
import com.phatjam98.elasticprotos.proto.SearchCondition
import com.phatjam98.elasticprotos.proto.SortingOrder
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.proto.example.TypedMaps
import com.phatjam98.elasticprotos.proto.test.Library
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class FilterCompositeAggregationSpec extends Specification {

  @Shared ProtoMapper mapper = ProtoMapper.withDefaults()
  @Shared IndexDefinition document = mapper.map(Document)
  @Shared IndexDefinition firm = mapper.map(Firm)
  @Shared IndexDefinition library = mapper.map(Library)
  @Shared IndexDefinition typedMaps = mapper.map(TypedMaps)
  @Shared QueryCompiler strict = QueryCompiler.withDefaults()
  @Shared QueryCompiler lenient = QueryCompiler.of(QueryCompilerConfig.standard().withLenient(true))

  static CompiledSearch compile(QueryCompiler compiler, IndexDefinition index, Object... aggs) {
    AggregationCompilerSpec.compile(compiler, index, aggs)
  }

  static Map aggs(IndexDefinition index, Object... aggs) {
    AggregationCompilerSpec.aggs(index, aggs)
  }

  static String error(IndexDefinition index, Object... aggs) {
    AggregationCompilerSpec.error(index, aggs)
  }

  // ---- filter ------------------------------------------------------------------------------

  def "a top-level filter compiles its condition like the query, nested fields wrapped in nested queries"() {
    expect:
    aggs(document, Aggs.sub(Aggs.filter('filings', Criteria.where('type').equalTo('FILING')
        .and('parties.role').equalTo('PLAINTIFF')), Aggs.sum('total', 'size_bytes'))).filings ==
        [filter: [bool: [filter: [[term: [type: 'FILING']],
                                  [nested: [path: 'parties', query: [term: ['parties.role': 'PLAINTIFF']]]]]]],
         aggs: [total: [sum: [field: 'size_bytes']]]]
  }

  def "a condition without predicates matches everything, but a filter needs a condition"() {
    expect:
    aggs(document, Aggs.filter('all', SearchCondition.defaultInstance)).all == [filter: [match_all: [:]]]
    error(document, Aggs.filter('none')).contains("Filter aggregation 'none' needs a condition")
    error(document, Aggs.filter('none', Criteria.builder())).contains("Filter aggregation 'none' needs a condition")
    error(document, Aggs.filters('k', Aggs.keyed('a'))).contains("Filters aggregation 'k' filter 'a' needs a condition")
    Aggs.filter('f', Criteria.where('type').equalTo('A')).build() ==
        Aggregation.newBuilder().setName('f').setFilter(FilterAggregation.newBuilder()
            .addAllCondition(Criteria.where('type').equalTo('A').conditions())).build()
  }

  def "OR branches are all kept: one condition per branch, compiled like the query's"() {
    when:
    def filter = aggs(document, Aggs.filter('f', Criteria.where('type').equalTo('FILING').or().where('type').equalTo('ORDER'))).f
    def keyed = aggs(document, Aggs.filters('k', Aggs.keyed('either', Criteria.where('type').equalTo('FILING')
        .or().where('tags').equalTo('urgent')))).k

    then:
    filter == [filter: [bool: [minimum_should_match: 1, should: [[bool: [filter: [[term: [type: 'FILING']]]]],
                                                                 [bool: [filter: [[term: [type: 'ORDER']]]]]]]]]
    keyed.filters.filters.either.bool.should.size() == 2
    aggs(document, Aggs.filter('g', Criteria.where('type').equalTo('A').end().fullText('x'))).g.filter.bool.keySet() ==
        ['filter', 'must'] as Set
  }

  def "a filter over nested objects enters the nested path and tests its fields without a nested query"() {
    when:
    def a = aggs(document, Aggs.sub(Aggs.filter('plaintiffs', 'parties', Criteria.where('parties.role').equalTo('PLAINTIFF')),
        Aggs.terms('names', 'parties.name'), Aggs.cardinality('docs', 'id')))

    then:
    a.keySet() == ['plaintiffs__nested'] as Set
    a.plaintiffs__nested == [nested: [path: 'parties'], aggs: [plaintiffs: [
        filter: [bool: [filter: [[term: ['parties.role': 'PLAINTIFF']]]]],
        aggs: [names: [terms: [field: 'parties.name.keyword']],
               docs__reverse: [reverse_nested: [:], aggs: [docs: [cardinality: [field: 'id']]]]]]]]
  }

  def "under a nested bucket a filter runs on the nested objects: same-path fields are bare, deeper ones nested"() {
    when:
    def a = aggs(library, Aggs.sub(Aggs.terms('labels', 'shelves.label'),
        Aggs.filter('front', Criteria.where('shelves.position').equalTo(1).and('shelves.books.rank').atLeast(2))))

    then:
    a.labels__nested.aggs.labels.aggs.front == [filter: [bool: [filter: [
        [term: ['shelves.position': 1L]],
        [nested: [path: 'shelves.books', query: [range: ['shelves.books.rank': [gte: 2L]]]]]]]]]
  }

  def "fields outside the nested context of a filter are refused"() {
    expect:
    error(document, Aggs.filter('f', 'parties', Criteria.where('type').equalTo('FILING')))
        .contains("Field 'type' is outside nested path 'parties', which filter aggregation 'f' runs in")
    error(document, Aggs.sub(Aggs.terms('roles', 'parties.role'), Aggs.filter('f', Criteria.where('firm_id').equalTo('x'))))
        .contains("Field 'firm_id' is outside nested path 'parties', which filter aggregation 'f' runs in")
    error(library, Aggs.sub(Aggs.terms('staff', 'staff.name'), Aggs.filter('f', Criteria.where('shelves.label').equalTo('x'))))
        .contains("Field 'shelves.label' is outside nested path 'staff'")
  }

  def "FULL_TEXT without fields in a nested context searches the full-text fields inside it"() {
    expect:
    aggs(document, Aggs.filter('ft', 'parties', Criteria.builder().condition().fullText('acme'))).ft__nested.aggs.ft.filter ==
        [bool: [must: [[multi_match: [fields: ['parties.name'], query: 'acme', type: 'best_fields']]]]]
    aggs(firm, Aggs.filter('ft', 'offices', Criteria.builder().condition().fullText('reno'))).ft__nested.aggs.ft.filter ==
        [bool: [must: [[multi_match: [fields: ['offices.name'], query: 'reno', type: 'best_fields']]]]]
  }

  def "EXISTS on a nested path the context has entered is always true, and its negation never"() {
    expect:
    aggs(library, Aggs.filter('f', 'shelves', Criteria.where('shelves').exists())).f__nested.aggs.f.filter ==
        [bool: [filter: [[match_all: [:]]]]]
    aggs(library, Aggs.filter('f', 'shelves', Criteria.where('shelves').not().exists())).f__nested.aggs.f.filter ==
        [bool: [must_not: [[match_all: [:]]]]]
    aggs(library, Aggs.filter('f', 'shelves', Criteria.where('shelves').isNull())).f__nested.aggs.f.filter ==
        [bool: [filter: [[bool: [must_not: [match_all: [:]]]]]]]
    aggs(library, Aggs.filter('f', 'shelves', Criteria.where('shelves.books').exists())).f__nested.aggs.f.filter ==
        [bool: [filter: [[nested: [path: 'shelves.books', query: [match_all: [:]]]]]]]
  }

  def "a filter takes no field or a nested path"() {
    expect:
    error(document, Aggs.filter('f', Criteria.where('type').equalTo('A')).setField('type'))
        .contains("Filter aggregation 'f' takes no field, or a nested path to filter the nested objects at that path, but 'type' is mapped as KEYWORD")
    error(document, Aggs.filter('f', Criteria.where('type').equalTo('A')).setField('nope'))
        .contains("Unknown field 'nope' in filter aggregation 'f'")
    error(typedMaps, Aggs.filter('f', Criteria.where('children.k.items.sku').equalTo('x')).setField('children.k.items'))
        .contains("inside nested path 'children.k.items' of a typed map")
  }

  def "lenient mode skips a whole filter or filters aggregation whose condition names an unknown field"() {
    when:
    def compiled = compile(lenient, document, Aggs.filter('f', Criteria.where('nope').equalTo('x').and('type').equalTo('A')),
        Aggs.filter('g', Criteria.where('type').equalTo('A')).setField('nope2'),
        Aggs.filters('k', Aggs.keyed('a', Criteria.where('type').equalTo('A')), Aggs.keyed('b', Criteria.builder().condition().fullText('x') {
          it.addFields(com.phatjam98.elasticprotos.proto.FieldBoost.newBuilder().setField('nope3'))
        })).tap { filtersBuilder.setOtherBucketKey('rest') },
        Aggs.filter('ok', Criteria.where('type').equalTo('A')))

    then:
    compiled.body().aggs == [ok: [filter: [bool: [filter: [[term: [type: 'A']]]]]]]
    compiled.warnings() == ["Skipped EQUALS on unknown field 'nope'",
                            "Skipped filter aggregation 'f': its condition names an unknown field",
                            "Skipped filter aggregation 'g' on unknown field 'nope2'",
                            "Skipped FULL_TEXT on unknown field 'nope3'",
                            "Skipped FULL_TEXT: none of its fields are mapped",
                            "Skipped filters aggregation 'k': its condition names an unknown field"]
  }

  def "conditions are typed and checked as in queries"() {
    expect:
    error(document, Aggs.filter('f', Criteria.where('size_bytes').equalTo('big'))).contains('cannot take string_value "big"')
    aggs(document, Aggs.filter('f', Criteria.where('title').equalTo('Motion'))).f.filter ==
        [bool: [filter: [[term: ['title.keyword': 'Motion']]]]]
  }

  // ---- filters -----------------------------------------------------------------------------

  def "filters compile one query per key, with the other bucket"() {
    expect:
    aggs(document, Aggs.filters('kinds', Aggs.keyed('filings', Criteria.where('type').equalTo('FILING')),
        Aggs.keyed('acme', Criteria.where('parties.name').matches('acme'))).tap { filtersBuilder.setOtherBucketKey('rest') }).kinds ==
        [filters: [filters: [acme: [bool: [must: [[nested: [path: 'parties', query: [match: ['parties.name': 'acme']]]]]]],
                             filings: [bool: [filter: [[term: [type: 'FILING']]]]]],
                   other_bucket_key: 'rest']]
  }

  def "filters over nested objects use the nested path as field"() {
    expect:
    aggs(document, Aggs.filters('roles', Aggs.keyed('p', Criteria.where('parties.role').equalTo('PLAINTIFF')))
        .setField('parties')).roles__nested.aggs.roles ==
        [filters: [filters: [p: [bool: [filter: [[term: ['parties.role': 'PLAINTIFF']]]]]]]]
  }

  @Unroll
  def "a filters aggregation is refused: #reason"() {
    expect:
    error(document, aggregation).contains(reason)

    where:
    aggregation                                                                                              || reason
    Aggs.filters('f')                                                                                        || "Filters aggregation 'f' needs at least one filter"
    Aggs.filters('f', KeyedFilter.newBuilder().addCondition(SearchCondition.defaultInstance).build())        || 'has a filter without a key'
    Aggs.filters('f', Aggs.keyed('a', SearchCondition.defaultInstance), Aggs.keyed('a', SearchCondition.defaultInstance)) || "uses the key 'a' twice"
    Aggs.filters('f', Aggs.keyed('a', SearchCondition.defaultInstance)).tap { filtersBuilder.setOtherBucketKey('a') } || "other_bucket_key 'a', which is also the key of a filter"
  }

  // ---- composite ---------------------------------------------------------------------------

  def "composite sources of every kind with order, missing bucket, size and after"() {
    given:
    def composite = Aggs.composite('pairs',
        Aggs.termsSource('firm', 'firm_id').setOrder(SortingOrder.DESC).setMissingBucket(true),
        Aggs.dateHistogramSource('month', 'filed_at', CalendarInterval.MONTH).tap { dateHistogramBuilder.setFormat('yyyy-MM').setTimeZone('UTC') },
        Aggs.histogramSource('size', 'size_bytes', 100_000).setOrder(SortingOrder.ASC),
        Aggs.termsSource('title', 'title'),
        Aggs.dateHistogramSource('hours', 'filed_at', '12h'))
    composite.compositeBuilder.setSize(50)
    Aggs.after(composite, [firm: null, month: '2024-02', size: 100000d, title: 'Motion', hours: 1700000000000L])

    expect:
    aggs(document, Aggs.sub(composite, Aggs.sum('total', 'size_bytes'))).pairs == [composite: [
        sources: [[firm: [terms: [field: 'firm_id', order: 'desc', missing_bucket: true]]],
                  [month: [date_histogram: [field: 'filed_at', calendar_interval: '1M', format: 'yyyy-MM', time_zone: 'UTC']]],
                  [size: [histogram: [field: 'size_bytes', interval: 100000L, order: 'asc']]],
                  [title: [terms: [field: 'title.keyword']]],
                  [hours: [date_histogram: [field: 'filed_at', fixed_interval: '12h']]]],
        size: 50,
        after: [firm: null, hours: 1700000000000L, month: '2024-02', size: 100000d, title: 'Motion']],
        aggs: [total: [sum: [field: 'size_bytes']]]]
  }

  def "a composite over nested sources is wrapped once, and its sub-aggregations run in that context"() {
    expect:
    aggs(document, Aggs.sub(Aggs.composite('roles', Aggs.termsSource('role', 'parties.role'), Aggs.termsSource('email', 'parties.email')),
        Aggs.cardinality('docs', 'id'))) == [roles__nested: [nested: [path: 'parties'], aggs: [roles: [
        composite: [sources: [[role: [terms: [field: 'parties.role']]], [email: [terms: [field: 'parties.email']]]]],
        aggs: [docs__reverse: [reverse_nested: [:], aggs: [docs: [cardinality: [field: 'id']]]]]]]]]
    aggs(library, Aggs.composite('books', Aggs.termsSource('name', 'shelves.books.name'))).books__nested.aggs.books__nested2 ==
        [nested: [path: 'shelves.books'], aggs: [books: [composite: [sources: [[name: [terms: [field: 'shelves.books.name']]]]]]]]
    aggs(firm, Aggs.composite('tiers', Aggs.termsSource('tier', 'labels.tier'))).tiers.composite.sources ==
        [[tier: [terms: [field: 'labels.tier']]]]
  }

  def "a composite must be top level, take no field, and keep its sources in one nested context"() {
    expect:
    error(document, Aggs.sub(Aggs.terms('t', 'type'), Aggs.composite('c', Aggs.termsSource('f', 'firm_id'))))
        .contains("Composite aggregation 'c' must be a top-level aggregation")
    error(document, Aggs.composite('c', Aggs.termsSource('f', 'firm_id')).setField('type'))
        .contains("Composite aggregation 'c' takes no field; its sources name their fields")
    error(document, Aggs.composite('c', Aggs.termsSource('f', 'firm_id'), Aggs.termsSource('r', 'parties.role')))
        .contains("Composite aggregation 'c' has sources in different nested contexts: 'f' in the root documents and 'r' in nested path 'parties'")
    error(library, Aggs.composite('c', Aggs.termsSource('s', 'shelves.label'), Aggs.termsSource('b', 'shelves.books.name')))
        .contains("'s' in nested path 'shelves' and 'b' in nested path 'shelves.books'")
    error(typedMaps, Aggs.composite('c', Aggs.termsSource('s', 'children.k.items.sku')))
        .contains("inside nested path 'children.k.items' of a typed map")
  }

  @Unroll
  def "a composite aggregation is refused: #reason"() {
    expect:
    error(document, aggregation).contains(reason)

    where:
    aggregation                                                                                         || reason
    Aggs.composite('c')                                                                                 || "Composite aggregation 'c' needs at least one source"
    Aggs.composite('c', CompositeSource.newBuilder().setField('type').setTerms(com.phatjam98.elasticprotos.proto.CompositeTermsSource.defaultInstance)) || 'has a source without a name'
    Aggs.composite('c', Aggs.termsSource('a', 'type'), Aggs.termsSource('a', 'tags'))                   || "uses the source name 'a' twice"
    Aggs.composite('c', Aggs.termsSource('a.b', 'type'))                                                || "Source name 'a.b' of composite aggregation 'c' must not contain '.'"
    Aggs.composite('c', Aggs.termsSource('a__b', 'type'))                                               || "Source name 'a__b' of composite aggregation 'c' must not contain '__'"
    Aggs.composite('c', Aggs.termsSource('a[0]', 'type'))                                               || "must not contain '['"
    Aggs.composite('c', Aggs.termsSource('a_', 'type'))                                                 || "Source name 'a_' of composite aggregation 'c' must not end with '_'"
    Aggs.composite('c', CompositeSource.newBuilder().setName('a').setField('type'))                     || "The source 'a' of composite aggregation 'c' sets no kind"
    Aggs.composite('c', Aggs.termsSource('a', ''))                                                      || "The source 'a' of composite aggregation 'c' needs a field"
    Aggs.composite('c', Aggs.termsSource('a', 'body'))                                                  || "Terms source 'a' of composite aggregation 'c' needs exact values"
    Aggs.composite('c', Aggs.termsSource('a', 'parties'))                                               || "cannot use 'parties' mapped as NESTED"
    Aggs.composite('c', Aggs.histogramSource('a', 'type', 1))                                           || "Histogram source 'a' of composite aggregation 'c' needs a numeric field but 'type' is mapped as KEYWORD"
    Aggs.composite('c', Aggs.histogramSource('a', 'size_bytes', 0))                                     || 'needs a positive, finite interval'
    Aggs.composite('c', Aggs.dateHistogramSource('a', 'size_bytes', CalendarInterval.DAY))              || "Date histogram source 'a' of composite aggregation 'c' needs a date field"
    Aggs.composite('c', Aggs.dateHistogramSource('a', 'filed_at', '1M'))                                || "Date histogram source 'a' of composite aggregation 'c' has fixed_interval '1M'"
    Aggs.composite('c', Aggs.dateHistogramSource('a', 'filed_at', CalendarInterval.DAY).tap { dateHistogramBuilder.setTimeZone('Nope/Nope') }) || "invalid time_zone 'Nope/Nope'"
    Aggs.composite('c', Aggs.termsSource('a', 'type').setOrderValue(9))                                 || "The source 'a' of composite aggregation 'c' has an unknown order"
    Aggs.composite('c', Aggs.termsSource('a', 'type')).tap { compositeBuilder.setSize(-1) }             || 'has a negative size: -1'
    Aggs.composite('c', Aggs.termsSource('a', 'type')).tap { compositeBuilder.setSize(1001) }           || 'asks for 1001 buckets per page, more than the maximum of 1000 (QueryCompilerConfig.maxTermsSize)'
    Aggs.after(Aggs.composite('c', Aggs.termsSource('a', 'type'), Aggs.termsSource('b', 'tags')), [a: 'x']) || 'has an after key with [a] but its sources are [a, b]'
  }

  def "flattened keys take only the terms source"() {
    expect:
    error(firm, Aggs.composite('c', Aggs.histogramSource('a', 'labels.tier', 1)))
        .contains("Histogram source 'a' of composite aggregation 'c' cannot use 'labels.tier', a key of a flattened field")
    error(firm, Aggs.composite('c', Aggs.dateHistogramSource('a', 'labels.tier', CalendarInterval.DAY)))
        .contains('a key of a flattened field')
  }

  def "an unknown source field throws in strict mode and skips the whole composite in lenient mode"() {
    expect:
    error(document, Aggs.composite('c', Aggs.termsSource('a', 'nope'))).contains("Unknown field 'nope' in composite aggregation 'c'")

    when:
    def compiled = compile(lenient, document, Aggs.composite('c', Aggs.termsSource('a', 'type'), Aggs.termsSource('b', 'nope')))

    then:
    !compiled.body().containsKey('aggs')
    compiled.warnings() == ["Skipped composite aggregation 'c' on unknown field 'nope'"]
  }

  def "after values keep their kind: null for a missing bucket, Long, Double, Boolean and String"() {
    given:
    def composite = Aggs.composite('c', Aggs.termsSource('a', 'type'), Aggs.termsSource('b', 'size_bytes'),
        Aggs.termsSource('c', 'relevance'), Aggs.termsSource('d', 'tags'), Aggs.termsSource('e', 'firm_id'))
    composite.compositeBuilder
        .putAfter('a', PrimitiveValue.defaultInstance)
        .putAfter('b', PrimitiveValue.newBuilder().setIntValue(7).build())
        .putAfter('c', PrimitiveValue.newBuilder().setNumberValue(0.5).build())
        .putAfter('d', PrimitiveValue.newBuilder().setBoolValue(true).build())
        .putAfter('e', PrimitiveValue.newBuilder().setStringValue('x').build())

    expect:
    aggs(document, composite).c.composite.after == [a: null, b: 7L, c: 0.5d, d: true, e: 'x']
  }

  def "Aggs.after replaces the after key and refuses values of other types"() {
    given:
    def composite = Aggs.composite('c', Aggs.termsSource('a', 'type'))

    when:
    Aggs.after(composite, [a: 'x'])
    Aggs.after(composite, [a: 'y'])

    then:
    composite.composite.afterMap == [a: PrimitiveValue.newBuilder().setStringValue('y').build()]

    when:
    Aggs.after(composite, [:])

    then:
    composite.composite.afterCount == 0

    when:
    Aggs.after(composite, [a: new Object()])

    then:
    thrown(IllegalArgumentException)
  }

  def "the kind list of an aggregation without a kind names the new kinds"() {
    expect:
    error(document, Aggregation.newBuilder().setName('x')).contains('set terms, date_histogram, histogram, range, metric, filter, filters or composite')
  }

  def "composite source builders"() {
    expect:
    Aggs.termsSource('a', 'f').build().hasTerms()
    Aggs.dateHistogramSource('a', 'f', CalendarInterval.DAY).build().dateHistogram.calendarInterval == CalendarInterval.DAY
    Aggs.dateHistogramSource('a', 'f', '1h').build().dateHistogram.fixedInterval == '1h'
    Aggs.histogramSource('a', 'f', 2.5).build().histogram.interval == 2.5d
    Aggs.composite('c', Aggs.termsSource('a', 'f')).build().composite.sourcesCount == 1
    Aggs.keyed('k', Criteria.where('type').equalTo('A')).conditionList*.fieldConditionCount == [1]
    Aggs.filter('f', Criteria.where('a').exists().or().where('b').exists().end()).filter.conditionCount == 2
    Aggs.filter('f', 'parties', Criteria.where('parties.role').equalTo('A')).field == 'parties'
  }
}
