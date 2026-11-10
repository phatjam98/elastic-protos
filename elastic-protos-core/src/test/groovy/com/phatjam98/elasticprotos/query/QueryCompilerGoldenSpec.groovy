package com.phatjam98.elasticprotos.query

import static com.phatjam98.elasticprotos.proto.SearchOperationType.*
import static com.phatjam98.elasticprotos.query.Q.*

import com.phatjam98.elasticprotos.Golden
import com.phatjam98.elasticprotos.mapping.IndexDefinition
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.DistanceType
import com.phatjam98.elasticprotos.proto.DistanceUnit
import com.phatjam98.elasticprotos.proto.FieldBoost
import com.phatjam98.elasticprotos.proto.FieldSort
import com.phatjam98.elasticprotos.proto.FlatValue
import com.phatjam98.elasticprotos.proto.FullTextOperator
import com.phatjam98.elasticprotos.proto.FullTextOptions
import com.phatjam98.elasticprotos.proto.FullTextType
import com.phatjam98.elasticprotos.proto.GeoBoundingBoxValue
import com.phatjam98.elasticprotos.proto.GeoDistanceSort
import com.phatjam98.elasticprotos.proto.GeoDistanceValue
import com.phatjam98.elasticprotos.proto.GeoShape
import com.phatjam98.elasticprotos.proto.Pagination
import com.phatjam98.elasticprotos.proto.ScoreSort
import com.phatjam98.elasticprotos.proto.ScriptSort
import com.phatjam98.elasticprotos.proto.ScriptSortType
import com.phatjam98.elasticprotos.proto.SearchCriteria
import com.phatjam98.elasticprotos.proto.SortingCriteria
import com.phatjam98.elasticprotos.proto.SortingOrder
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.proto.example.TypedMaps
import com.phatjam98.elasticprotos.proto.test.Geo
import com.phatjam98.elasticprotos.proto.test.Library
import com.phatjam98.elasticprotos.proto.CalendarInterval
import com.phatjam98.elasticprotos.proto.DateBounds
import com.phatjam98.elasticprotos.proto.HistogramAggregation
import com.phatjam98.elasticprotos.proto.TermsAggregation
import com.phatjam98.elasticprotos.proto.TermsOrder
import com.phatjam98.elasticprotos.proto.TermsOrderBy
import com.google.protobuf.util.Timestamps
import java.time.Instant
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class QueryCompilerGoldenSpec extends Specification {

  @Shared ProtoMapper mapper = ProtoMapper.withDefaults()
  @Shared IndexDefinition document = mapper.map(Document)
  @Shared IndexDefinition firm = mapper.map(Firm)
  @Shared IndexDefinition geo = mapper.map(Geo)
  @Shared IndexDefinition typedMaps = mapper.map(TypedMaps)
  @Shared IndexDefinition library = mapper.map(Library)
  @Shared QueryCompiler strict = QueryCompiler.withDefaults()
  @Shared QueryCompiler wildcards =
      QueryCompiler.of(QueryCompilerConfig.standard().withAllowLeadingWildcards(true))
  @Shared QueryCompiler permissive = QueryCompiler.of(
      QueryCompilerConfig.standard().withAllowRegex(true).withAllowScriptSort(true))
  @Shared QueryCompiler ungrouped =
      QueryCompiler.of(QueryCompilerConfig.standard().withGroupNested(false))

  static FlatValue shape(String wkt, String geojson = '') {
    FlatValue.newBuilder().setGeoShapeValue(GeoShape.newBuilder().setWkt(wkt).setGeojson(geojson)).build()
  }

  @Unroll
  def "operation #name compiles to its golden body"() {
    when:
    CompiledSearch compiled = compiler.compile(criteria, index)

    then:
    compiled.warnings().isEmpty()
    Golden.assertMatches("query/op_${name}", compiled.body())

    where:
    name                    | compiler  | index    | criteria
    'equals_keyword'        | strict    | document | where(fc('type', EQUALS, str('FILING')))
    'equals_text_keyword'   | strict    | firm     | where(fc('name', EQUALS, str('Acme LLP')))
    'equals_long'           | strict    | document | where(fc('size_bytes', EQUALS, num(2048)))
    'equals_null'           | strict    | document | where(fc('firm_id', EQUALS, nul()))
    'exists'                | strict    | document | where(fc('checksum', EXISTS))
    'text_matches'          | strict    | document | where(fc('title', TEXT_MATCHES, str('motion to dismiss')))
    'text_contains_text'    | strict    | document | where(fc('body', TEXT_CONTAINS, str('breach of contract')))
    'text_contains_keyword' | wildcards | document | where(fc('tags', TEXT_CONTAINS, str('urg*nt')))
    'text_prefix'           | strict    | firm     | where(fc('name', TEXT_PREFIX, str('Acm')))
    'text_suffix'           | wildcards | firm     | where(fc('domain', TEXT_SUFFIX, str('.com')))
    'text_regex'            | permissive | document | where(fc('firm_id', TEXT_REGEX, str('f[0-9]+')))
    'num_range'             | strict    | document | where(fc('size_bytes', NUM_RANGE, range(100, 1000, true, false)))
    'num_range_date'        | strict    | document | where(fc('filed_at', NUM_RANGE, range(1.7e12, 1.8e12)))
    'num_range_min_only'    | strict    | document | where(fc('size_bytes', NUM_RANGE, Values.atLeast(100)))
    'num_range_max_only'    | strict    | document | where(fc('size_bytes', NUM_RANGE, Values.lessThan(1000)))
    'num_range_date_after'  | strict    | document | where(fc('filed_at', NUM_RANGE, Values.after(Instant.ofEpochMilli(1700000000000L))))
    'num_range_date_on_or_before' | strict | document | where(fc('filed_at', NUM_RANGE, Values.onOrBefore(Instant.ofEpochMilli(1800000000000L))))
    'any_in'                | strict    | document | where(fc('tags', ANY_IN, list('urgent', 'sealed')))
    'geo_distance'          | strict    | firm     | where(fc('headquarters.location', GEO_DISTANCE, geoDistance()))
    'geo_bounding_box'      | strict    | geo      | where(fc('point', GEO_BOUNDING_BOX, box()))
    'geo_bounding_box_wkt'  | strict    | geo      | where(fc('point', GEO_BOUNDING_BOX, wktBox()))
    'geo_intersects'        | strict    | geo      | where(fc('shape', GEO_INTERSECTS, shape('', '{"type":"Point","coordinates":[-115.1,36.1]}')))
    'geo_contains'          | strict    | geo      | where(fc('shape', GEO_CONTAINS, shape('POINT (-115.1 36.1)')))
    'geo_contained'         | strict    | geo      | where(fc('wkt_shape', GEO_CONTAINED, FlatValue.newBuilder().setLatLonValue(ll(36.1, -115.1)).build()))
    'negation'              | strict    | document | where(fc('type', EQUALS, str('ORDER'), true), fc('title', TEXT_MATCHES, str('appeal')))
    'full_text_options'     | strict    | document | where(fullText('breach', fullTextOptions()))
    'map_key_equals_long'   | strict    | typedMaps | where(fc('counters.views', EQUALS, integer(5)))
    'map_key_num_range'     | strict    | typedMaps | where(fc('counters.views', NUM_RANGE, range(10, 100)))
    'map_key_geo_distance'  | strict    | typedMaps | where(fc('places.home', GEO_DISTANCE, geoDistance()))
    'map_key_full_text'     | strict    | typedMaps | where(fullText('quarterly reports'), fc('notes.summary', TEXT_MATCHES, str('report')))
    'map_key_nested_message' | strict   | typedMaps | where(fc('children.alice.items.sku', EQUALS, str('A-1')), fc('children.alice.items.quantity', NUM_RANGE, range(1, 5)), fc('children.alice.count', EQUALS, integer(3)))
  }

  def "op_map_key_unknown_subfield names the value type"() {
    when:
    strict.compile(where(fc('children.alice.nope', EQUALS, str('x'))), typedMaps)

    then:
    QueryCompileException e = thrown()
    e.field == 'children.alice.nope'
    e.message == "Unknown field 'children.alice.nope' in EQUALS; the values of map 'children' in index " +
        "typed_maps are phatjam98.elasticprotos.example.TypedChild, which does not map it"
  }

  static FlatValue geoDistance() {
    FlatValue.newBuilder().setGeoDistanceValue(GeoDistanceValue.newBuilder()
        .setPoint(ll(36.17, -115.14)).setDistance('25km').setDistanceType(DistanceType.PLANE)).build()
  }

  static FlatValue box() {
    FlatValue.newBuilder().setGeoBoundingBoxValue(GeoBoundingBoxValue.newBuilder()
        .setTopLeft(ll(37, -116)).setBottomRight(ll(35, -114))).build()
  }

  static FlatValue wktBox() {
    FlatValue.newBuilder().setGeoBoundingBoxValue(GeoBoundingBoxValue.newBuilder()
        .setWkt('BBOX (-116, -114, 37, 35)')).build()
  }

  static FullTextOptions fullTextOptions() {
    FullTextOptions.newBuilder()
        .addFields(FieldBoost.newBuilder().setField('title').setBoost(2.5f))
        .addFields(FieldBoost.newBuilder().setField('body'))
        .setOperator(FullTextOperator.OPERATOR_AND)
        .setFuzziness('AUTO')
        .setType(FullTextType.MOST_FIELDS)
        .setMinimumShouldMatchPct(75)
        .build()
  }

  @Unroll
  def "sort #name compiles to its golden body"() {
    expect:
    Golden.assertMatches("query/sort_${name}", permissive.compile(sorted(sort), index).body())

    where:
    name           | index    | sort
    'field'        | document | SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder().setFieldName('filed_at').setOrder(SortingOrder.DESC)).build()
    'field_text'   | firm     | SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder().setFieldName('name')).build()
    'field_nested' | firm     | SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder().setFieldName('offices.headcount').setOrder(SortingOrder.ASC)).build()
    'score'        | document | SortingCriteria.newBuilder().setScoreSort(ScoreSort.newBuilder().setOrder(SortingOrder.DESC)).build()
    'geo_distance' | firm     | SortingCriteria.newBuilder().setGeoDistanceSort(GeoDistanceSort.newBuilder().setFieldName('headquarters.location').setPoint(ll(36.17, -115.14)).setDistanceUnit(DistanceUnit.MILES)).build()
    'script'       | document | SortingCriteria.newBuilder().setScriptSort(ScriptSort.newBuilder().setSource("doc['size_bytes'].value * 2").setSortType(ScriptSortType.NUMBER).setOrder(SortingOrder.DESC)).build()
  }

  @Unroll
  def "scenario #name compiles to its golden body"() {
    expect:
    Golden.assertMatches("query/${name}", compiler.compile(criteria, index).body())

    where:
    name                     | compiler  | index    | criteria
    'match_all'              | strict    | document | SearchCriteria.defaultInstance
    'full_text_document'     | strict    | document | where(fullText('breach of contract'))
    'full_text_firm'         | strict    | firm     | where(fullText('acme'), fc('status', EQUALS, str('ACTIVE')))
    'nested_grouped'         | strict    | firm     | nestedCriteria()
    'nested_ungrouped'       | ungrouped | firm     | nestedCriteria()
    'multi_condition_or'     | strict    | document | anyOf(and(fc('type', EQUALS, str('FILING')), fc('tags', EQUALS, str('urgent'))), and(fc('size_bytes', NUM_RANGE, range(0, 10)), fc('parties.role', EQUALS, str('COUNSEL'), true)))
    'paging_from_size'       | strict    | document | paged(Pagination.newBuilder().setFrom(40).setSize(20).build(), where(fc('type', EQUALS, str('FILING'))))
    'paging_cursor'          | strict    | document | paged(Pagination.newBuilder().setSize(50).setCursor('pit-123').setKeepAlive('5m').addSearchAfter(prim(1700000000000L)).addSearchAfter(prim('doc-9')).addSearchAfter(prim(42L)).build(), sorted(SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder().setFieldName('filed_at').setOrder(SortingOrder.DESC)).build()))
  }

  static SearchCriteria nestedCriteria() {
    where(
        fc('offices.name', TEXT_MATCHES, str('Las Vegas')),
        fc('name', EQUALS, str('Acme LLP')),
        fc('offices.headcount', NUM_RANGE, range(10, 100)),
        fc('offices.address.city', EQUALS, str('Reno'), true))
  }

  @Unroll
  def "aggregation #name compiles to its golden body"() {
    when:
    CompiledSearch compiled = strict.compile(criteria.build(), index)

    then:
    compiled.warnings().isEmpty()
    Golden.assertMatches("query/agg_${name}", compiled.body())

    where:
    name                       | index    | criteria
    'terms_keyword'            | document | Criteria.builder().aggregate(Aggs.terms('by_type', 'type'))
    'terms_text_keyword'       | firm     | Criteria.builder().aggregate(Aggs.terms('names', 'name'))
    'terms_options'            | document | Criteria.builder().aggregate(Aggs.terms('tags', 'tags').setTerms(TermsAggregation.newBuilder().setSize(5).setMinDocCount(2).setMissing(prim('none')).addOrder(TermsOrder.newBuilder().setBy(TermsOrderBy.BY_KEY).setDirection(SortingOrder.DESC))))
    'terms_order_metric'       | document | Criteria.builder().aggregate(Aggs.sub(Aggs.terms('by_type', 'type').setTerms(TermsAggregation.newBuilder().addOrder(TermsOrder.newBuilder().setBy(TermsOrderBy.BY_METRIC).setMetric('total'))), Aggs.sum('total', 'size_bytes'), Aggs.avg('average', 'size_bytes')))
    'terms_flattened_key'      | firm     | Criteria.builder().aggregate(Aggs.terms('tiers', 'labels.tier'))
    'date_histogram_calendar'  | document | Criteria.builder().aggregate(Aggs.dateHistogram('per_month', 'filed_at', CalendarInterval.MONTH).tap { dateHistogramBuilder.setTimeZone('Europe/Berlin').setFormat('yyyy-MM') })
    'date_histogram_fixed_bounds' | document | Criteria.builder().aggregate(Aggs.dateHistogram('half_days', 'filed_at', '12h').tap { dateHistogramBuilder.setMinDocCount(0).setExtendedBounds(DateBounds.newBuilder().setMin(Timestamps.fromMillis(1704067200000L)).setMax(Timestamps.fromMillis(1719791999000L))) })
    'histogram'                | document | Criteria.builder().aggregate(Aggs.histogram('sizes', 'size_bytes', 100000).setHistogram(HistogramAggregation.newBuilder().setInterval(100000).setMinDocCount(1).setOffset(50)))
    'range_numeric'            | document | Criteria.builder().aggregate(Aggs.range('sizes', 'size_bytes', Aggs.bucket('small', null, 50000d), Aggs.bucket('medium', 50000d, 500000d), Aggs.bucket(null, 500000d, null)))
    'range_date'               | document | Criteria.builder().aggregate(Aggs.range('q1', 'filed_at', Aggs.bucket('q1_2024', 1704067200000d, 1711929600000d)))
    'metrics_all'              | document | Criteria.builder().aggregate(Aggs.min('smallest', 'size_bytes'), Aggs.max('latest', 'filed_at'), Aggs.avg('average', 'size_bytes'), Aggs.sum('total', 'size_bytes'), Aggs.count('titled', 'title'), Aggs.cardinality('firms', 'firm_id'))
    'nested_terms'             | document | Criteria.builder().aggregate(Aggs.terms('roles', 'parties.role'))
    'nested_reverse_metric'    | document | Criteria.builder().aggregate(Aggs.sub(Aggs.terms('roles', 'parties.role'), Aggs.cardinality('firms', 'firm_id'), Aggs.count('emails', 'parties.email')))
    'nested_order_reverse_metric' | document | Criteria.builder().aggregate(Aggs.sub(Aggs.terms('roles', 'parties.role').setTerms(TermsAggregation.newBuilder().addOrder(TermsOrder.newBuilder().setBy(TermsOrderBy.BY_METRIC).setMetric('firms').setDirection(SortingOrder.ASC))), Aggs.cardinality('firms', 'firm_id')))
    'nested_divergent'         | library  | Criteria.builder().aggregate(Aggs.sub(Aggs.terms('books', 'shelves.books.name'), Aggs.max('staff_rank', 'staff.rank'), Aggs.max('position', 'shelves.position'), Aggs.sum('ranks', 'shelves.books.rank')), Aggs.sub(Aggs.terms('cities', 'city'), Aggs.count('book_count', 'shelves.books.name')))
    'only_size_zero'           | document | Criteria.where('type').equalTo('FILING').end().aggregate(Aggs.terms('tags', 'tags')).aggregationsOnly()
    'three_levels'             | document | Criteria.builder().aggregate(Aggs.sub(Aggs.terms('by_type', 'type'), Aggs.sub(Aggs.terms('roles', 'parties.role').setTerms(TermsAggregation.newBuilder().addOrder(TermsOrder.newBuilder().setBy(TermsOrderBy.BY_METRIC).setMetric('firms'))), Aggs.cardinality('firms', 'firm_id'))))
    'filter_root'              | document | Criteria.builder().aggregate(Aggs.sub(Aggs.filter('filings', Criteria.where('type').equalTo('FILING').and('parties.role').equalTo('PLAINTIFF')), Aggs.sum('total', 'size_bytes')))
    'filter_nested_objects'    | document | Criteria.builder().aggregate(Aggs.sub(Aggs.filter('plaintiffs', 'parties', Criteria.where('parties.role').equalTo('PLAINTIFF')), Aggs.terms('names', 'parties.name'), Aggs.cardinality('docs', 'id')))
    'filter_in_nested_context' | library  | Criteria.builder().aggregate(Aggs.sub(Aggs.terms('labels', 'shelves.label'), Aggs.filter('front', Criteria.where('shelves.position').equalTo(1).and('shelves.books.rank').atLeast(2))))
    'filters_other'            | document | Criteria.builder().aggregate(Aggs.sub(Aggs.filters('kinds', Aggs.keyed('filings', Criteria.where('type').equalTo('FILING')), Aggs.keyed('acme', Criteria.where('parties.name').matches('acme'))).tap { filtersBuilder.setOtherBucketKey('rest') }, Aggs.max('latest', 'filed_at')))
    'composite'                | document | Criteria.builder().aggregate(Aggs.sub(Aggs.after(Aggs.composite('pairs', Aggs.termsSource('firm', 'firm_id').setMissingBucket(true), Aggs.dateHistogramSource('month', 'filed_at', CalendarInterval.MONTH).tap { dateHistogramBuilder.setFormat('yyyy-MM') }, Aggs.histogramSource('size', 'size_bytes', 100000).setOrder(SortingOrder.DESC)).tap { compositeBuilder.setSize(50) }, [firm: 'firm-2', month: '2024-02', size: 0L]), Aggs.sum('total', 'size_bytes'))).aggregationsOnly()
    'composite_nested'         | document | Criteria.builder().aggregate(Aggs.sub(Aggs.composite('roles', Aggs.termsSource('role', 'parties.role'), Aggs.termsSource('name', 'parties.name')), Aggs.cardinality('docs', 'id')))
  }

  @Unroll
  def "highlight #name compiles to its golden body"() {
    when:
    CompiledSearch compiled = strict.compile(criteria.build(), document)

    then:
    compiled.warnings().isEmpty()
    Golden.assertMatches("query/${name}", compiled.body())

    where:
    name                       | criteria
    'highlight_default_fields' | Criteria.builder().fullText('merger agreement').highlight()
    'highlight_options'        | Criteria.builder().fullText('merger').highlight { it.addFields(com.phatjam98.elasticprotos.proto.HighlightField.newBuilder().setField('title').setNumberOfFragments(0)).addFields(com.phatjam98.elasticprotos.proto.HighlightField.newBuilder().setField('body').setFragmentSize(80).setNumberOfFragments(2)).setFragmentSize(150).setNumberOfFragments(3).addPreTags('<b>').addPostTags('</b>').setRequireFieldMatch(false).setHtmlEncode(true) }
    'search_aggs_and_highlight' | Criteria.where('type').equalTo('FILING').end().fullText('agreement').sortByScore(SortingOrder.DESC).page(20, 0).aggregate(Aggs.sub(Aggs.terms('by_firm', 'firm_id').setTerms(TermsAggregation.newBuilder().setSize(3)), Aggs.max('latest', 'filed_at')), Aggs.dateHistogram('per_month', 'filed_at', CalendarInterval.MONTH)).highlight('title')
  }
}
