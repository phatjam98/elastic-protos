package com.phatjam98.elasticprotos.query

import static com.phatjam98.elasticprotos.proto.SearchOperationType.*
import static com.phatjam98.elasticprotos.query.Q.*

import com.google.protobuf.Timestamp
import com.phatjam98.elasticprotos.mapping.IndexDefinition
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.DistanceUnit
import com.phatjam98.elasticprotos.proto.FieldBoost
import com.phatjam98.elasticprotos.proto.FieldCondition
import com.phatjam98.elasticprotos.proto.FieldSort
import com.phatjam98.elasticprotos.proto.FlatValue
import com.phatjam98.elasticprotos.proto.FullTextOperator
import com.phatjam98.elasticprotos.proto.FullTextOptions
import com.phatjam98.elasticprotos.proto.GeoBoundingBoxValue
import com.phatjam98.elasticprotos.proto.GeoDistanceSort
import com.phatjam98.elasticprotos.proto.GeoDistanceValue
import com.phatjam98.elasticprotos.proto.Pagination
import com.phatjam98.elasticprotos.proto.PrimitiveValue
import com.phatjam98.elasticprotos.proto.ScoreSort
import com.phatjam98.elasticprotos.proto.SearchCriteria
import com.phatjam98.elasticprotos.proto.SortingCriteria
import com.phatjam98.elasticprotos.proto.SortingOrder
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.DocumentType
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.proto.test.Geo
import java.time.Instant
import spock.lang.Shared
import spock.lang.Specification

class CriteriaSpec extends Specification {

  @Shared ProtoMapper mapper = ProtoMapper.withDefaults()
  @Shared IndexDefinition document = mapper.map(Document)
  @Shared IndexDefinition firm = mapper.map(Firm)
  @Shared IndexDefinition geo = mapper.map(Geo)
  @Shared QueryCompiler compiler = QueryCompiler.of(
      QueryCompilerConfig.standard().withAllowLeadingWildcards(true).withAllowRegex(true))

  static final Instant FROM = Instant.parse('2024-02-01T00:00:00Z')
  static final Instant TO = Instant.parse('2024-04-01T00:00:00Z')
  static final String POINT_JSON = '{"type":"Point","coordinates":[-115.1,36.1]}'

  /** Builder output and hand-built criteria are the same message and compile to the same body. */
  void same(SearchCriteria built, SearchCriteria expected, IndexDefinition index) {
    assert built == expected
    assert compiler.compile(built, index).body() == compiler.compile(expected, index).body()
  }

  def "#description"() {
    given:
    def branch = Criteria.builder().condition()
    step.call(branch)

    expect:
    same(branch.build(), where(expected), index)

    where:
    description                     | index    | step                                                                   | expected
    'equalTo a string'              | document | { it.where('type').equalTo('MOTION') }                                 | fc('type', EQUALS, str('MOTION'))
    'equalTo an enum constant'      | document | { it.where('type').equalTo(DocumentType.MOTION) }                      | fc('type', EQUALS, str('MOTION'))
    'equalTo a long'                | document | { it.where('size_bytes').equalTo(5L) }                                 | fc('size_bytes', EQUALS, integer(5))
    'equalTo a double'              | document | { it.where('relevance').equalTo(0.5d) }                                | fc('relevance', EQUALS, num(0.5))
    'equalTo a boolean'             | document | { it.where('tags').equalTo(true) }                                     | fc('tags', EQUALS, bool(true))
    'equalTo an instant'            | document | { it.where('filed_at').equalTo(Instant.ofEpochSecond(1_700_000_000L)) } | fc('filed_at', EQUALS, ts(1_700_000_000L))
    'equalTo a timestamp'           | document | { it.where('filed_at').equalTo(Timestamp.newBuilder().setSeconds(9).build()) } | fc('filed_at', EQUALS, ts(9))
    'equalTo a FlatValue'           | document | { it.where('tags').equalTo(Values.of('x')) }                           | fc('tags', EQUALS, str('x'))
    'equalTo null'                  | document | { it.where('relevance').equalTo(null) }                                | fc('relevance', EQUALS, nul())
    'isNull'                        | document | { it.where('relevance').isNull() }                                     | fc('relevance', EQUALS, nul())
    'not isNull'                    | document | { it.where('relevance').not().isNull() }                               | fc('relevance', EQUALS, nul(), true)
    'exists'                        | document | { it.where('relevance').exists() }                                     | FieldCondition.newBuilder().setField('relevance').setOperation(EXISTS).build()
    'not exists'                    | document | { it.where('relevance').not().exists() }                               | FieldCondition.newBuilder().setField('relevance').setOperation(EXISTS).setNegate(true).build()
    'matches'                       | document | { it.where('body').matches('summary judgment') }                       | fc('body', TEXT_MATCHES, str('summary judgment'))
    'contains'                      | document | { it.where('body').contains('state a claim') }                         | fc('body', TEXT_CONTAINS, str('state a claim'))
    'startsWith'                    | document | { it.where('tags').startsWith('civ') }                                 | fc('tags', TEXT_PREFIX, str('civ'))
    'endsWith'                      | document | { it.where('tags').endsWith('vil') }                                   | fc('tags', TEXT_SUFFIX, str('vil'))
    'regex'                         | document | { it.where('id').regex('doc-[13]') }                                   | fc('id', TEXT_REGEX, str('doc-[13]'))
    'between numbers'               | document | { it.where('size_bytes').between(10_000, 100_000) }                    | fc('size_bytes', NUM_RANGE, range(10_000, 100_000))
    'between instants'              | document | { it.where('filed_at').between(FROM, TO) }                             | fc('filed_at', NUM_RANGE, range(FROM.toEpochMilli(), TO.toEpochMilli()))
    'atLeast'                       | document | { it.where('size_bytes').atLeast(5) }                                  | fc('size_bytes', NUM_RANGE, Values.atLeast(5))
    'greaterThan'                   | document | { it.where('size_bytes').greaterThan(5) }                              | fc('size_bytes', NUM_RANGE, Values.greaterThan(5))
    'atMost'                        | document | { it.where('size_bytes').atMost(5) }                                   | fc('size_bytes', NUM_RANGE, Values.atMost(5))
    'lessThan'                      | document | { it.where('size_bytes').lessThan(5) }                                 | fc('size_bytes', NUM_RANGE, Values.lessThan(5))
    'onOrAfter'                     | document | { it.where('filed_at').onOrAfter(FROM) }                               | fc('filed_at', NUM_RANGE, Values.onOrAfter(FROM))
    'after'                         | document | { it.where('filed_at').after(FROM) }                                   | fc('filed_at', NUM_RANGE, Values.after(FROM))
    'onOrBefore'                    | document | { it.where('filed_at').onOrBefore(TO) }                                | fc('filed_at', NUM_RANGE, Values.onOrBefore(TO))
    'before'                        | document | { it.where('filed_at').before(TO) }                                    | fc('filed_at', NUM_RANGE, Values.before(TO))
    'not atLeast'                   | document | { it.where('size_bytes').not().atLeast(5) }                            | fc('size_bytes', NUM_RANGE, Values.atLeast(5), true)
    'range'                         | document | { it.where('size_bytes').range(1, false, 9, true) }                    | fc('size_bytes', NUM_RANGE, range(1, 9, false, true))
    'not range'                     | document | { it.where('size_bytes').not().range(1, true, 9, false) }              | fc('size_bytes', NUM_RANGE, range(1, 9, true, false), true)
    'anyOf'                         | document | { it.where('tags').anyOf('urgent', 'admin') }                          | fc('tags', ANY_IN, list('urgent', 'admin'))
    'anyOf enum constants'          | document | { it.where('type').anyOf(DocumentType.MOTION, 'FILING') }              | fc('type', ANY_IN, list('MOTION', 'FILING'))
    'not anyOf'                     | document | { it.where('tags').not().anyOf('draft') }                              | fc('tags', ANY_IN, list('draft'), true)
    'withinDistance'                | firm     | { it.where('headquarters.location').withinDistance(36.1, -115.1, '50km') } | fc('headquarters.location', GEO_DISTANCE, FlatValue.newBuilder().setGeoDistanceValue(GeoDistanceValue.newBuilder().setPoint(ll(36.1, -115.1)).setDistance('50km')).build())
    'inBoundingBox by corners'      | firm     | { it.where('headquarters.location').inBoundingBox(38, -120, 34, -104) } | fc('headquarters.location', GEO_BOUNDING_BOX, FlatValue.newBuilder().setGeoBoundingBoxValue(GeoBoundingBoxValue.newBuilder().setTopLeft(ll(38, -120)).setBottomRight(ll(34, -104))).build())
    'inBoundingBox by wkt'          | firm     | { it.where('headquarters.location').inBoundingBox('BBOX (-120, -104, 38, 34)') } | fc('headquarters.location', GEO_BOUNDING_BOX, FlatValue.newBuilder().setGeoBoundingBoxValue(GeoBoundingBoxValue.newBuilder().setWkt('BBOX (-120, -104, 38, 34)')).build())
    'intersects'                    | geo      | { it.where('shape').intersects(Values.wkt('POINT (-115.1 36.1)')) }    | fc('shape', GEO_INTERSECTS, Values.wkt('POINT (-115.1 36.1)'))
    'containsShape'                 | geo      | { it.where('shape').containsShape(Values.geoJson(POINT_JSON)) }        | fc('shape', GEO_CONTAINS, Values.geoJson(POINT_JSON))
    'withinShape'                   | geo      | { it.where('shape').withinShape(Values.latLon(36.1, -115.1)) }         | fc('shape', GEO_CONTAINED, Values.latLon(36.1, -115.1))
    'field fullText'                | document | { it.where('title').fullText('merger') }                               | fullText('merger', FullTextOptions.defaultInstance, 'title')
    'field fullText with options'   | document | { it.where('title').fullText('merger') { o -> o.setOperator(FullTextOperator.OPERATOR_AND) } } | fullText('merger', FullTextOptions.newBuilder().setOperator(FullTextOperator.OPERATOR_AND).build(), 'title')
    'not field fullText'            | document | { it.where('title').not().fullText('merger') }                         | fullText('merger', FullTextOptions.defaultInstance, 'title').toBuilder().setNegate(true).build()
    'branch fullText'               | document | { it.fullText('merger agreement') }                                    | fullText('merger agreement')
    'branch fullText with options'  | document | { it.fullText('merger') { o -> o.addFields(FieldBoost.newBuilder().setField('body')) } } | fullText('merger', FullTextOptions.newBuilder().addFields(FieldBoost.newBuilder().setField('body')).build())
  }

  def "predicates chain with and, branches with or, and the criteria takes sorting and paging"() {
    when:
    SearchCriteria built = Criteria.where('type').equalTo(DocumentType.FILING)
        .and('tags').not().equalTo('discovery')
        .or()
        .where('firm_id').equalTo('firm-2')
        .end()
        .sortBy('filed_at', SortingOrder.DESC)
        .sortByScore(SortingOrder.DESC)
        .page(20, 40)
        .build()

    then:
    same(built, SearchCriteria.newBuilder()
        .addSearchCondition(and(fc('type', EQUALS, str('FILING')), fc('tags', EQUALS, str('discovery'), true)))
        .addSearchCondition(and(fc('firm_id', EQUALS, str('firm-2'))))
        .addSortingCriteria(SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder()
            .setFieldName('filed_at').setOrder(SortingOrder.DESC)))
        .addSortingCriteria(SortingCriteria.newBuilder().setScoreSort(ScoreSort.newBuilder()
            .setOrder(SortingOrder.DESC)))
        .setPagination(Pagination.newBuilder().setSize(20).setFrom(40))
        .build(), document)
  }

  def "condition opens branches on the builder"() {
    given:
    def criteria = Criteria.builder()
    criteria.condition().where('type').equalTo('MOTION')
    criteria.condition().where('type').equalTo('ORDER')

    expect:
    same(criteria.build(), anyOf(and(fc('type', EQUALS, str('MOTION'))), and(fc('type', EQUALS, str('ORDER')))),
        document)
  }

  def "a top-level fullText is added to every branch, before or after the branches"() {
    when:
    SearchCriteria built = Criteria.builder()
        .fullText('merger agreement')
        .condition().where('type').equalTo('MOTION')
        .or().where('type').equalTo('ORDER')
        .build()

    then:
    same(built, anyOf(and(fc('type', EQUALS, str('MOTION')), fullText('merger agreement')),
        and(fc('type', EQUALS, str('ORDER')), fullText('merger agreement'))), document)
  }

  def "a top-level fullText alone is a single branch"() {
    expect:
    same(Criteria.builder().fullText('merger agreement').pageSize(5).build(),
        paged(Pagination.newBuilder().setSize(5).build(), where(fullText('merger agreement'))), document)
  }

  def "an empty builder matches everything"() {
    expect:
    Criteria.builder().build() == SearchCriteria.defaultInstance
    compiler.compile(Criteria.builder().build(), document).body().query == [match_all: [:]]
  }

  def "sortByDistance sorts by distance from a point"() {
    when:
    SearchCriteria built = Criteria.builder()
        .sortByDistance('headquarters.location', 36.1, -115.1, DistanceUnit.KILOMETERS, SortingOrder.ASC)
        .build()

    then:
    same(built, sorted(SortingCriteria.newBuilder().setGeoDistanceSort(GeoDistanceSort.newBuilder()
        .setFieldName('headquarters.location').setPoint(ll(36.1, -115.1))
        .setDistanceUnit(DistanceUnit.KILOMETERS).setOrder(SortingOrder.ASC)).build()), firm)
  }

  def "cursor pages over a point in time with search_after"() {
    when:
    SearchCriteria built = Criteria.builder()
        .sortBy('id', SortingOrder.ASC)
        .pageSize(100)
        .cursor('pit-1', '1m', ['stale'])
        .cursor('pit-2', '2m', ['doc-9', 42L, 1.5d, true])
        .build()
    Pagination expected = Pagination.newBuilder().setSize(100).setCursor('pit-2').setKeepAlive('2m')
        .addSearchAfter(PrimitiveValue.newBuilder().setStringValue('doc-9'))
        .addSearchAfter(PrimitiveValue.newBuilder().setIntValue(42))
        .addSearchAfter(PrimitiveValue.newBuilder().setNumberValue(1.5))
        .addSearchAfter(PrimitiveValue.newBuilder().setBoolValue(true))
        .build()

    then:
    same(built, paged(expected, sorted(SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder()
        .setFieldName('id').setOrder(SortingOrder.ASC)).build())), document)
    compiler.compile(built, document).body().pit.id == 'pit-2'
    compiler.compile(built, document).body().search_after == ['doc-9', 42L, 1.5d, true]
  }

  def "invalid input fails where the criteria are built"() {
    when:
    action.call()

    then:
    thrown(exception)

    where:
    [action, exception] << [
        [{ -> Criteria.where('tags').anyOf(new Object()) }, IllegalArgumentException],
        [{ -> Criteria.where('tags').equalTo([1, 2]) }, IllegalArgumentException],
        [{ -> Criteria.builder().cursor('p', '1m', [[a: 1]]) }, IllegalArgumentException],
        [{ -> Criteria.where(null) }, NullPointerException],
        [{ -> Criteria.builder().sortBy(null, SortingOrder.ASC) }, NullPointerException],
        [{ -> Criteria.builder().fullText(null) }, NullPointerException],
    ]
  }

  def "the builder output is the generated wire message and can be refined with toBuilder"() {
    given:
    SearchCriteria built = Criteria.where('type').equalTo('MOTION').build()

    expect:
    built.toBuilder().setPagination(Pagination.newBuilder().setSize(1)).build().pagination.size == 1
    built.getSearchCondition(0).getFieldCondition(0) instanceof FieldCondition
  }
}
