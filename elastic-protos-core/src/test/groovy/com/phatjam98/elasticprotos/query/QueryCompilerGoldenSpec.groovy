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
import com.phatjam98.elasticprotos.proto.test.Geo
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class QueryCompilerGoldenSpec extends Specification {

  @Shared ProtoMapper mapper = ProtoMapper.withDefaults()
  @Shared IndexDefinition document = mapper.map(Document)
  @Shared IndexDefinition firm = mapper.map(Firm)
  @Shared IndexDefinition geo = mapper.map(Geo)
  @Shared QueryCompiler strict = QueryCompiler.withDefaults()
  @Shared QueryCompiler wildcards =
      QueryCompiler.of(QueryCompilerConfig.standard().withAllowLeadingWildcards(true))
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
    'text_regex'            | strict    | document | where(fc('firm_id', TEXT_REGEX, str('f[0-9]+')))
    'num_range'             | strict    | document | where(fc('size_bytes', NUM_RANGE, range(100, 1000, true, false)))
    'num_range_date'        | strict    | document | where(fc('filed_at', NUM_RANGE, range(1.7e12, 1.8e12)))
    'any_in'                | strict    | document | where(fc('tags', ANY_IN, list('urgent', 'sealed')))
    'geo_distance'          | strict    | firm     | where(fc('headquarters.location', GEO_DISTANCE, geoDistance()))
    'geo_bounding_box'      | strict    | geo      | where(fc('point', GEO_BOUNDING_BOX, box()))
    'geo_bounding_box_wkt'  | strict    | geo      | where(fc('point', GEO_BOUNDING_BOX, wktBox()))
    'geo_intersects'        | strict    | geo      | where(fc('shape', GEO_INTERSECTS, shape('', '{"type":"Point","coordinates":[-115.1,36.1]}')))
    'geo_contains'          | strict    | geo      | where(fc('shape', GEO_CONTAINS, shape('POINT (-115.1 36.1)')))
    'geo_contained'         | strict    | geo      | where(fc('wkt_shape', GEO_CONTAINED, FlatValue.newBuilder().setLatLonValue(ll(36.1, -115.1)).build()))
    'negation'              | strict    | document | where(fc('type', EQUALS, str('ORDER'), true), fc('title', TEXT_MATCHES, str('appeal')))
    'full_text_options'     | strict    | document | where(fullText('breach', fullTextOptions()))
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
    Golden.assertMatches("query/sort_${name}", strict.compile(sorted(sort), index).body())

    where:
    name           | index    | sort
    'field'        | document | SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder().setFieldName('filed_at').setOrder(SortingOrder.DESC)).build()
    'field_text'   | firm     | SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder().setFieldName('name')).build()
    'field_nested' | firm     | SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder().setFieldName('offices.headcount').setOrder(SortingOrder.ASC)).build()
    'score'        | document | SortingCriteria.newBuilder().setScoreSort(ScoreSort.newBuilder().setOrder(SortingOrder.DESC)).build()
    'geo_distance' | firm     | SortingCriteria.newBuilder().setGeoDistanceSort(GeoDistanceSort.newBuilder().setFieldName('headquarters.location').setPoint(ll(36.17, -115.14)).setDistanceUnit(DistanceUnit.MILES)).build()
    'script'       | document | SortingCriteria.newBuilder().setScriptSort(ScriptSort.newBuilder().setScriptOrId("doc['size_bytes'].value * 2").setSortType(ScriptSortType.NUMBER).setOrder(SortingOrder.DESC)).build()
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
}
