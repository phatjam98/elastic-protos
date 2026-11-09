package com.phatjam98.elasticprotos.query

import static com.phatjam98.elasticprotos.proto.SearchOperationType.*
import static com.phatjam98.elasticprotos.query.Q.*

import com.phatjam98.elasticprotos.mapping.FieldInfo
import com.phatjam98.elasticprotos.mapping.FieldKind
import com.phatjam98.elasticprotos.mapping.IndexDefinition
import com.phatjam98.elasticprotos.mapping.MappingOverrides
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.EsType
import com.phatjam98.elasticprotos.proto.FieldBoost
import com.phatjam98.elasticprotos.proto.FieldCondition
import com.phatjam98.elasticprotos.proto.FieldSort
import com.phatjam98.elasticprotos.proto.FlatValue
import com.phatjam98.elasticprotos.proto.FullTextOptions
import com.phatjam98.elasticprotos.proto.GeoBoundingBoxValue
import com.phatjam98.elasticprotos.proto.GeoDistanceSort
import com.phatjam98.elasticprotos.proto.GeoDistanceValue
import com.phatjam98.elasticprotos.proto.GeoShape
import com.phatjam98.elasticprotos.proto.Pagination
import com.phatjam98.elasticprotos.proto.ScriptSort
import com.phatjam98.elasticprotos.proto.ScriptSortType
import com.phatjam98.elasticprotos.proto.SearchCriteria
import com.phatjam98.elasticprotos.proto.SearchOperationType
import com.phatjam98.elasticprotos.proto.SortingCriteria
import com.phatjam98.elasticprotos.proto.SortingOrder
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.proto.test.Annotated
import com.phatjam98.elasticprotos.proto.test.Composite
import com.phatjam98.elasticprotos.proto.test.Geo
import com.phatjam98.elasticprotos.proto.test.Scalars
import com.phatjam98.elasticprotos.proto.test.WellKnown
import java.time.Duration
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class QueryCompilerSpec extends Specification {

  @Shared ProtoMapper mapper = ProtoMapper.withDefaults()
  @Shared IndexDefinition document = mapper.map(Document)
  @Shared IndexDefinition firm = mapper.map(Firm)
  @Shared IndexDefinition scalars = mapper.map(Scalars)
  @Shared IndexDefinition wellKnown = mapper.map(WellKnown)
  @Shared IndexDefinition geo = mapper.map(Geo)
  @Shared IndexDefinition annotated = mapper.map(Annotated)
  @Shared QueryCompiler compiler = QueryCompiler.withDefaults()
  @Shared QueryCompiler regexAllowed = QueryCompiler.of(QueryCompilerConfig.standard().withAllowRegex(true))
  @Shared QueryCompiler scriptsAllowed =
      QueryCompiler.of(QueryCompilerConfig.standard().withAllowScriptSort(true))

  /** The single filter clause of a one-condition query. */
  Map filter(IndexDefinition index, FieldCondition... conditions) {
    compiler.compileQuery(where(conditions), index).bool.filter[0] as Map
  }

  Object term(IndexDefinition index, String field, FlatValue value) {
    Map clause = filter(index, fc(field, EQUALS, value))
    clause.term.values().first()
  }

  QueryCompileException failure(Closure work) {
    try {
      work()
    } catch (QueryCompileException e) {
      return e
    }
    throw new AssertionError('expected a QueryCompileException')
  }

  // ---- typed values ------------------------------------------------------------------------

  @Unroll
  def "#field with #value.kindCase is typed as #expected (#type)"() {
    when:
    Object typed = term(index, field, value)

    then:
    typed == expected
    typed.getClass() == type

    where:
    index     | field            | value                   || expected                   | type
    scalars   | 'int64_value'    | num(5)                  || 5L                         | Long
    scalars   | 'int32_value'    | integer(7)              || 7L                         | Long
    scalars   | 'int64_value'    | integer(9007199254740993L) || 9007199254740993L     | Long
    scalars   | 'int64_value'    | num(5.5)                || 5.5d                       | Double
    scalars   | 'int64_value'    | str(' 42 ')             || 42L                        | Long
    scalars   | 'int64_value'    | str('1.0')              || 1L                         | Long
    scalars   | 'double_value'   | num(5)                  || 5.0d                       | Double
    scalars   | 'double_value'   | integer(5)              || 5.0d                       | Double
    scalars   | 'float_value'    | str('2.5')              || 2.5d                       | Double
    scalars   | 'bool_value'     | bool(true)              || true                       | Boolean
    scalars   | 'bool_value'     | str('FALSE')            || false                      | Boolean
    scalars   | 'string_value'   | str('x')                || 'x'                        | String
    scalars   | 'string_value'   | num(3)                  || '3'                        | String
    scalars   | 'string_value'   | num(3.25)               || '3.25'                     | String
    scalars   | 'string_value'   | integer(12)             || '12'                       | String
    scalars   | 'string_value'   | bool(false)             || 'false'                    | String
    scalars   | 'bytes_value'    | str('AAE=')             || 'AAE='                     | String
    annotated | 'ip'             | str('10.0.0.1')         || '10.0.0.1'                 | String
    wellKnown | 'timestamp_value' | ts(1700000000, 5000000) || '2023-11-14T22:13:20.005Z' | String
    wellKnown | 'timestamp_value' | str('2024-01-01')      || '2024-01-01'               | String
    wellKnown | 'timestamp_value' | integer(1700000000000) || '2023-11-14T22:13:20Z'     | String
    wellKnown | 'timestamp_value' | num(1700000000000)     || '2023-11-14T22:13:20Z'     | String
  }

  @Unroll
  def "#field rejects #value.kindCase"() {
    when:
    def e = failure { term(index, field, value) }

    then:
    e.field == field
    e.message.contains("'${field}'")
    e.message.contains(kind)
    e.message.contains(given)

    where:
    index     | field             | value               || kind        | given
    scalars   | 'int64_value'     | str('abc')          || 'LONG'      | 'string_value "abc"'
    scalars   | 'double_value'    | str('NaN')          || 'DOUBLE'    | 'string_value'
    scalars   | 'double_value'    | bool(true)          || 'DOUBLE'    | 'bool_value'
    scalars   | 'bool_value'      | str('yes')          || 'BOOLEAN'   | 'string_value'
    scalars   | 'bool_value'      | num(1)              || 'BOOLEAN'   | 'number_value'
    scalars   | 'string_value'    | list('a')           || 'KEYWORD'   | 'list_value'
    scalars   | 'string_value'    | FlatValue.defaultInstance || 'KEYWORD' | 'no value'
    wellKnown | 'timestamp_value' | bool(true)          || 'DATE'      | 'bool_value'
  }

  def "a date range treats bounds as epoch millis"() {
    expect:
    filter(document, fc('filed_at', NUM_RANGE, range(1000, 2000.5, false, true))).range.filed_at ==
        [gt: 1000L, lte: 2000.5d, format: 'epoch_millis']
  }

  def "an out of range timestamp is reported"() {
    when:
    def e = failure { term(wellKnown, 'timestamp_value', ts(Long.MAX_VALUE)) }

    then:
    e.message.contains('outside the supported range')
  }

  def "ANY_IN types every item and accepts a scalar as a one-element list"() {
    expect:
    filter(scalars, fc('int64_value', ANY_IN, list(1L, 2.5d, '3'))).terms.int64_value == [1L, 2.5d, 3L]
    filter(firm, fc('name', ANY_IN, str('Acme'))).terms == ['name.keyword': ['Acme']]
    filter(wellKnown, fc('timestamp_value', ANY_IN, list(0L))).terms.timestamp_value == ['1970-01-01T00:00:00Z']
  }

  def "an unset list item fails the type check"() {
    expect:
    failure { filter(scalars, fc('string_value', ANY_IN, list('a', null))) }.message.contains('no value')
    failure { filter(scalars, fc('string_value', ANY_IN, list())) }.message.contains('at least one value')
  }

  // ---- exact matching on text --------------------------------------------------------------

  def "EQUALS on text uses the keyword sub-field when there is one"() {
    expect:
    filter(document, fc('title', EQUALS, str('Order'))) == [term: ['title.keyword': 'Order']]
  }

  @Unroll
  def "#op on text without keyword sub-field points at the option"() {
    when:
    def e = failure { regexAllowed.compile(where(fc('body', op, str('x'))), document) }

    then:
    e.field == 'body'
    e.message.contains('[(elastic_field).keyword_subfield = TRUE]')

    where:
    op << [EQUALS, TEXT_PREFIX, TEXT_REGEX, TEXT_SUFFIX, ANY_IN]
  }

  def "sorting on text without keyword sub-field is rejected"() {
    when:
    def e = failure { compiler.compile(sorted(fieldSort('body')), document) }

    then:
    e.field == 'body'
    e.message.contains('keyword_subfield = TRUE')
  }

  def "TEXT_MATCHES on keyword is a non-scoring filter, on text it scores"() {
    when:
    Map bool = compiler.compileQuery(where(fc('tags', TEXT_MATCHES, str('a')),
        fc('title', TEXT_MATCHES, str('b'))), document).bool

    then:
    bool.filter == [[match: [tags: 'a']]]
    bool.must == [[match: [title: 'b']]]
  }

  @Unroll
  def "#op is rejected on #field mapped as #kind"() {
    when:
    def e = failure { compiler.compile(where(fc(field, op, value)), index) }

    then:
    e.field == field
    e.message.contains(kind)

    where:
    index    | field                   | op               | value          || kind
    document | 'size_bytes'            | TEXT_MATCHES     | str('1')       || 'LONG'
    document | 'size_bytes'            | TEXT_PREFIX      | str('1')       || 'LONG'
    document | 'size_bytes'            | TEXT_CONTAINS    | str('1')       || 'LONG'
    document | 'type'                  | NUM_RANGE        | range(0, 1)    || 'KEYWORD'
    document | 'type'                  | GEO_DISTANCE     | str('x')       || 'KEYWORD'
    document | 'type'                  | GEO_BOUNDING_BOX | str('x')       || 'KEYWORD'
    firm     | 'headquarters.location' | GEO_INTERSECTS   | str('x')       || 'GEO_POINT'
    firm     | 'headquarters.location' | EQUALS           | str('x')       || 'GEO_POINT'
    firm     | 'headquarters'          | ANY_IN           | str('x')       || 'OBJECT'
  }

  @Unroll
  def "#op rejects a malformed operand: #message"() {
    when:
    def e = failure { compiler.compile(where(fc(field, op, value)), index) }

    then:
    e.message.contains(message)

    where:
    index    | field                   | op               | value                                     || message
    document | 'size_bytes'            | NUM_RANGE        | num(3)                                    || 'needs range_value but got number_value'
    firm     | 'headquarters.location' | GEO_DISTANCE     | FlatValue.newBuilder().setLatLonValue(ll(1, 2)).build() || 'needs geo_distance_value'
    firm     | 'headquarters.location' | GEO_DISTANCE     | FlatValue.newBuilder().setGeoDistanceValue(GeoDistanceValue.newBuilder().setPoint(ll(1, 2))).build() || 'distance such as'
    geo      | 'point'                 | GEO_BOUNDING_BOX | FlatValue.newBuilder().setGeoBoundingBoxValue(GeoBoundingBoxValue.newBuilder().setTopLeft(ll(1, 2))).build() || 'both top_left and bottom_right'
    geo      | 'point'                 | GEO_BOUNDING_BOX | str('x')                                  || 'got string_value'
    geo      | 'shape'                 | GEO_INTERSECTS   | str('x')                                  || 'needs geo_shape_value or lat_lon_value'
    geo      | 'shape'                 | GEO_INTERSECTS   | FlatValue.newBuilder().setGeoShapeValue(GeoShape.defaultInstance).build() || 'neither wkt nor geojson'
    geo      | 'shape'                 | GEO_INTERSECTS   | FlatValue.newBuilder().setGeoShapeValue(GeoShape.newBuilder().setGeojson('[1]')).build() || 'not a JSON object'
  }

  def "operations without a field or with an unsupported operation fail"() {
    expect:
    failure { compiler.compile(where(fc('', EQUALS, str('x'))), document) }.message
        .contains('only FULL_TEXT may leave it empty')
    failure { compiler.compile(where(fc('type', SEARCH_UNSUPPORTED)), document) }.message
        .contains('Unsupported operation SEARCH_UNSUPPORTED')
  }

  // ---- leading wildcards -------------------------------------------------------------------

  @Unroll
  def "#op on a keyword field is gated by allowLeadingWildcards"() {
    when:
    def e = failure { compiler.compile(where(fc('tags', op, str('a?b\\c'))), document) }

    then:
    e.field == 'tags'
    e.message.contains('allowLeadingWildcards')

    when:
    def allowed = QueryCompiler.of(QueryCompilerConfig.standard().withAllowLeadingWildcards(true))
    Map clause = allowed.compileQuery(where(fc('tags', op, str('a?b\\c'))), document).bool.filter[0]

    then:
    clause == [wildcard: [tags: [value: pattern]]]

    where:
    op            || pattern
    TEXT_CONTAINS || '*a\\?b\\\\c*'
    TEXT_SUFFIX   || '*a\\?b\\\\c'
  }

  def "TEXT_CONTAINS on text needs no wildcard"() {
    expect:
    compiler.compileQuery(where(fc('title', TEXT_CONTAINS, str('x'))), document).bool.must ==
        [[match_phrase: [title: 'x']]]
  }

  // ---- unknown fields ----------------------------------------------------------------------

  // ---- keys of flattened fields -------------------------------------------------------------

  def "a key of a flattened map field compiles to keyword-style term-level queries"() {
    expect:
    firm.fields()['labels'].kind() == FieldKind.FLATTENED
    filter(firm, fc('labels.env', EQUALS, str('prod'))) == [term: ['labels.env': 'prod']]
    filter(firm, fc('labels.env', ANY_IN, list('prod', 'staging'))) == [terms: ['labels.env': ['prod', 'staging']]]
    filter(firm, fc('labels.env', EXISTS)) == [exists: [field: 'labels.env']]
    filter(firm, fc('labels.env', TEXT_PREFIX, str('pr'))) == [prefix: ['labels.env': [value: 'pr']]]
    filter(firm, fc('labels.env', EQUALS, nul())) == [bool: [must_not: [exists: [field: 'labels.env']]]]
    filter(firm, fc('labels.team.name', EQUALS, str('x'))) == [term: ['labels.team.name': 'x']]
    filter(firm, fc('labels.count', EQUALS, integer(3))) == [term: ['labels.count': '3']]

    and: 'negation and the flattened root itself work as before'
    compiler.compileQuery(where(fc('labels.env', EQUALS, str('prod'), true)), firm).bool.must_not ==
        [[term: ['labels.env': 'prod']]]
    filter(firm, fc('labels', EQUALS, str('prod'))) == [term: [labels: 'prod']]

    and: 'a Struct mapped as flattened takes keys the same way'
    filter(document, fc('metadata.pages', EQUALS, str('12'))) == [term: ['metadata.pages': '12']]
  }

  @Unroll
  def "#op on a key of a flattened field is refused"() {
    when:
    def e = failure { compiler.compile(where(fc('labels.env', op, value)), firm) }

    then:
    e.field == 'labels.env'
    e.message == "${op} cannot be used on 'labels.env', a key of flattened field 'labels'; keys take EQUALS, ANY_IN, EXISTS and TEXT_PREFIX".toString()

    where:
    op            | value
    NUM_RANGE     | range(1, 2)
    TEXT_MATCHES  | str('prod')
    TEXT_CONTAINS | str('prod')
    TEXT_REGEX    | str('p.*')
  }

  def "paths that are not keys of a flattened field stay unknown"() {
    given:
    def objectLabels = ProtoMapper.builder().overrides(MappingOverrides.builder()
        .forField(Firm.descriptor, 'labels') { it.type = EsType.OBJECT }.build()).build().map(Firm)

    expect: 'a key of a map opted in to a dynamic object'
    failure { compiler.compile(where(fc('labels.env', EQUALS, str('x'))), objectLabels) }.message ==
        "Unknown field 'labels.env' in EQUALS; index firms does not map it"

    and: 'a trailing dot and a key under a non-flattened field'
    failure { compiler.compile(where(fc('labels.', EQUALS, str('x'))), firm) }.field == 'labels.'
    failure { compiler.compile(where(fc('headquarters.nope', EQUALS, str('x'))), firm) }.field == 'headquarters.nope'

    and: 'FULL_TEXT does not search keys'
    failure { compiler.compile(where(fullText('x', FullTextOptions.newBuilder()
        .addFields(FieldBoost.newBuilder().setField('labels.env')).build())), firm) }.field == 'labels.env'
  }

  def "an unknown field throws in strict mode"() {
    when:
    def e = failure { compiler.compile(where(fc('nope', EQUALS, str('x'))), document) }

    then:
    e.field == 'nope'
    e.message == "Unknown field 'nope' in EQUALS; index documents does not map it"
  }

  def "an unknown field is skipped with a warning in lenient mode"() {
    given:
    def lenient = QueryCompiler.of(QueryCompilerConfig.standard().withLenient(true))
    def criteria = where(fc('nope', EQUALS, str('x')), fc('type', EQUALS, str('FILING')))
        .toBuilder().addSortingCriteria(fieldSort('missing'))
        .addSortingCriteria(SortingCriteria.newBuilder().setGeoDistanceSort(
            GeoDistanceSort.newBuilder().setFieldName('gone'))).build()

    when:
    CompiledSearch compiled = lenient.compile(criteria, document)

    then:
    compiled.warnings() == [
        "Skipped EQUALS on unknown field 'nope'",
        "Skipped sort on unknown field 'missing'",
        "Skipped sort on unknown field 'gone'"]
    compiled.body().query == [bool: [filter: [[term: [type: 'FILING']]]]]
    !compiled.body().containsKey('sort')
  }

  def "a condition left empty in lenient mode matches everything"() {
    given:
    def lenient = QueryCompiler.of(QueryCompilerConfig.standard().withLenient(true))

    expect:
    lenient.compileQuery(where(fc('nope', EXISTS)), document) == [match_all: [:]]
  }

  def "unknown sort fields throw in strict mode"() {
    expect:
    failure { compiler.compile(sorted(fieldSort('nope')), document) }.field == 'nope'
    failure {
      compiler.compile(sorted(SortingCriteria.newBuilder().setGeoDistanceSort(
          GeoDistanceSort.newBuilder().setFieldName('nope')).build()), document)
    }.field == 'nope'
  }

  // ---- FULL_TEXT ---------------------------------------------------------------------------

  def "FULL_TEXT with explicit fields validates them"() {
    given:
    def options = FullTextOptions.newBuilder()
        .addFields(FieldBoost.newBuilder().setField(field)).build()

    when:
    def e = failure { compiler.compile(where(fullText('x', options)), document) }

    then:
    e.field == field
    e.message.contains(message)

    where:
    field        || message
    'size_bytes' || 'mapped as LONG'
    'nope'       || "Unknown field 'nope' in FULL_TEXT"
  }

  def "FULL_TEXT with only unknown fields is skipped in lenient mode"() {
    given:
    def lenient = QueryCompiler.of(QueryCompilerConfig.standard().withLenient(true))
    def options = FullTextOptions.newBuilder().addFields(FieldBoost.newBuilder().setField('nope')).build()

    when:
    CompiledSearch compiled = lenient.compile(where(fullText('x', options)), document)

    then:
    compiled.body().query == [match_all: [:]]
    compiled.warnings() == ["Skipped FULL_TEXT on unknown field 'nope'",
                            'Skipped FULL_TEXT: none of its fields are mapped']
  }

  def "FULL_TEXT on a named field searches only that field"() {
    expect:
    compiler.compileQuery(where(fullText('x', FullTextOptions.defaultInstance, 'parties.name')), document) ==
        [bool: [must: [[nested: [path: 'parties', query: [multi_match: [
            query: 'x', fields: ['parties.name'], type: 'best_fields']]]]]]]
  }

  def "FULL_TEXT needs a query and some full-text fields"() {
    expect:
    failure {
      compiler.compile(where(fc('', FULL_TEXT, num(1))), document)
    }.message.contains('non-empty string_value')
    failure { compiler.compile(where(fullText('x')), scalars) }.message.contains('has none')
  }

  def "negated FULL_TEXT goes to must_not"() {
    given:
    def condition = fullText('x').toBuilder().setNegate(true).build()

    expect:
    compiler.compileQuery(where(condition), document).bool.must_not.size() == 1
  }

  def "boost suffixes drop default boosts"() {
    expect:
    ConditionCompiler.boostSuffix(0f) == ''
    ConditionCompiler.boostSuffix(1f) == ''
    ConditionCompiler.boostSuffix(3f) == '^3'
    ConditionCompiler.boostSuffix(0.5f) == '^0.5'
  }

  // ---- nesting -----------------------------------------------------------------------------

  IndexDefinition twoLevels() {
    MappingOverrides overrides = MappingOverrides.builder()
        .forPath('children.grandchild') { it.setType(EsType.NESTED) }
        .build()
    mapper.map(Composite.descriptor, overrides)
  }

  def "a field under two nested levels is wrapped outermost first"() {
    given:
    IndexDefinition index = twoLevels()

    expect:
    index.nestedPaths() == ['children', 'children.grandchild'] as Set
    filter(index, fc('children.grandchild.note', EQUALS, str('n'))) ==
        [nested: [path : 'children',
                  query: [nested: [path : 'children.grandchild',
                                   query: [term: ['children.grandchild.note': 'n']]]]]]
  }

  def "a sort under two nested levels chains nested paths"() {
    expect:
    compiler.compile(sorted(fieldSort('children.grandchild.note')), twoLevels()).body().sort ==
        [['children.grandchild.note': [order: 'asc',
                                       nested: [path: 'children', nested: [path: 'children.grandchild']]]]]
  }

  def "grouping only joins clauses with the same innermost nested path"() {
    when:
    Map bool = compiler.compileQuery(where(
        fc('children.name', EQUALS, str('a')),
        fc('children.grandchild.note', EQUALS, str('n')),
        fc('children.rank', EQUALS, num(1))), twoLevels()).bool

    then:
    bool.filter == [
        [nested: [path: 'children', query: [bool: [filter: [
            [term: ['children.name': 'a']], [term: ['children.rank': 1L]]]]]]],
        [nested: [path: 'children', query: [nested: [path: 'children.grandchild',
                                                     query: [term: ['children.grandchild.note': 'n']]]]]]]
  }

  def "negated-only nested groups are not combined"() {
    when:
    Map bool = compiler.compileQuery(where(
        fc('offices.name', EQUALS, str('a'), true),
        fc('offices.headcount', EQUALS, num(1), true)), firm).bool

    then:
    bool.must_not == [
        [nested: [path: 'offices', query: [term: ['offices.name.keyword': 'a']]]],
        [nested: [path: 'offices', query: [term: ['offices.headcount': 1L]]]]]
  }

  def "dotted paths through objects are never wrapped"() {
    expect:
    filter(firm, fc('headquarters.city', EQUALS, str('Reno'))) == [term: ['headquarters.city': 'Reno']]
  }

  def "EXISTS on a nested field tests for at least one element"() {
    expect:
    filter(firm, fc('offices', EXISTS)) == [nested: [path: 'offices', query: [match_all: [:]]]]
    filter(firm, fc('offices', EQUALS, nul())) ==
        [bool: [must_not: [nested: [path: 'offices', query: [match_all: [:]]]]]]
  }

  // ---- is null -----------------------------------------------------------------------------

  def "EQUALS null means missing, negated it means present"() {
    expect:
    filter(document, fc('firm_id', EQUALS, nul())) == [bool: [must_not: [exists: [field: 'firm_id']]]]
    filter(document, fc('firm_id', EQUALS, nul(), true)) == [exists: [field: 'firm_id']]
    filter(document, fc('parties.email', EQUALS, nul())) ==
        [nested: [path: 'parties', query: [bool: [must_not: [exists: [field: 'parties.email']]]]]]
  }

  // ---- sorts -------------------------------------------------------------------------------

  def "sort details"() {
    given:
    def unset = SortingCriteria.newBuilder().setGeoDistanceSort(GeoDistanceSort.newBuilder()
        .setFieldName('offices.address.location').setOrder(SortingOrder.DESC)).build()
    def script = SortingCriteria.newBuilder().setScriptSort(ScriptSort.newBuilder()
        .setSource('x').setSortType(ScriptSortType.STRING)).build()

    when:
    List sort = scriptsAllowed.compile(sorted(fieldSort('_id'), unset, script), firm).body().sort

    then:
    sort[0] == [_id: [order: 'asc']]
    sort[1] == [_geo_distance: ['offices.address.location': [lat: 0d, lon: 0d], order: 'desc', unit: 'km',
                                distance_type: 'arc', nested: [path: 'offices']]]
    sort[2]._script.type == 'string'
  }

  @Unroll
  def "distance unit #unit is #expected"() {
    expect:
    SortCompiler.unit(unit) == expected

    where:
    unit << com.phatjam98.elasticprotos.proto.DistanceUnit.values().findAll { it.name() != 'UNRECOGNIZED' }
    expected << ['km', 'in', 'yd', 'ft', 'km', 'nmi', 'mm', 'cm', 'mi', 'm']
  }

  def "invalid sorts are rejected"() {
    expect:
    failure { compiler.compile(sorted(fieldSort('headquarters.location')), firm) }.message.contains('geo distance sort')
    failure { compiler.compile(sorted(fieldSort('offices')), firm) }.message.contains('NESTED')
    failure {
      compiler.compile(sorted(SortingCriteria.newBuilder().setGeoDistanceSort(
          GeoDistanceSort.newBuilder().setFieldName('domain')).build()), firm)
    }.message.contains('needs a GEO_POINT')
    failure {
      scriptsAllowed.compile(sorted(SortingCriteria.newBuilder().setScriptSort(ScriptSort.defaultInstance).build()), firm)
    }.message.contains('set source (inline Painless) or id (stored script)')
    failure {
      scriptsAllowed.compile(sorted(scriptSort(ScriptSort.newBuilder().setSource(''))), firm)
    }.message.contains('empty source')
    failure {
      scriptsAllowed.compile(sorted(scriptSort(ScriptSort.newBuilder().setId(''))), firm)
    }.message.contains('empty stored script id')
    failure { compiler.compile(sorted(SortingCriteria.defaultInstance), firm) }.message.contains('no sort type')
  }

  // ---- paging ------------------------------------------------------------------------------

  def "size defaults, count-only and track_total_hits"() {
    given:
    def custom = QueryCompiler.of(QueryCompilerConfig.standard().withDefaultSize(25).withTrackTotalHits(TrackTotalHits.engineDefault()))

    expect:
    custom.compile(SearchCriteria.defaultInstance, document).body() == [query: [match_all: [:]], size: 25, from: 0]
    compiler.compile(paged(Pagination.newBuilder().setSize(-1).build()), document).body().size == 0
    QueryCompiler.of(QueryCompilerConfig.standard().withTrackTotalHits(TrackTotalHits.upTo(1000)))
        .compile(SearchCriteria.defaultInstance, document).body().track_total_hits == Integer.valueOf(1000)
    QueryCompiler.of(QueryCompilerConfig.standard().withTrackTotalHits(TrackTotalHits.none()))
        .compile(SearchCriteria.defaultInstance, document).body().track_total_hits == false
    compiler.compile(SearchCriteria.defaultInstance, document).body().track_total_hits == true
    QueryCompiler.of(QueryCompilerConfig.standard().withTrackTotalHits(TrackTotalHits.upTo(1000)))
        .compile(SearchCriteria.defaultInstance, document).body().track_total_hits instanceof Integer
  }

  def "from + size beyond the result window advises a point in time"() {
    when:
    def e = failure { compiler.compile(paged(Pagination.newBuilder().setFrom(9995).setSize(10).build()), document) }

    then:
    e.message.contains('exceeds the result window of 10000')
    e.message.contains('point in time')

    expect:
    compiler.compile(paged(Pagination.newBuilder().setFrom(9990).setSize(10).build()), document).body().from == 9990
  }

  def "a smaller configured result window applies"() {
    expect:
    failure {
      QueryCompiler.of(QueryCompilerConfig.standard().withMaxResultWindow(50))
          .compile(paged(Pagination.newBuilder().setFrom(45).setSize(10).build()), document)
    }.message.contains('result window of 50')
    failure { compiler.compile(paged(Pagination.newBuilder().setFrom(-1).build()), document) }
        .message.contains('must not be negative')
  }

  def "a cursor uses the point in time with the _shard_doc tiebreaker"() {
    given:
    def custom = QueryCompiler.of(QueryCompilerConfig.standard().withDefaultKeepAlive(Duration.ofSeconds(90)))

    when:
    CompiledSearch compiled = custom.compile(paged(Pagination.newBuilder().setCursor('p1').setFrom(5).build(),
        sorted(fieldSort('filed_at'))), document)

    then:
    compiled.usesPointInTime()
    compiled.body().pit == [id: 'p1', keep_alive: '90s']
    !compiled.body().containsKey('from')
    !compiled.body().containsKey('search_after')
    compiled.body().sort == [[filed_at: [order: 'asc']], [_shard_doc: [order: 'asc']]]
  }

  def "an explicit _shard_doc sort is not duplicated"() {
    when:
    CompiledSearch compiled = compiler.compile(paged(Pagination.newBuilder().setCursor('p1').build(),
        sorted(SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder()
            .setFieldName('_shard_doc').setOrder(SortingOrder.DESC)).build())), document)

    then:
    compiled.body().sort == [[_shard_doc: [order: 'desc']]]
    !QueryCompiler.withDefaults().compile(SearchCriteria.defaultInstance, document).usesPointInTime()
  }

  def "without the _shard_doc tiebreaker a cursor search warns instead"() {
    given:
    def noShardDoc = QueryCompiler.of(QueryCompilerConfig.standard().withShardDocTiebreaker(false))

    when:
    CompiledSearch compiled = noShardDoc.compile(paged(Pagination.newBuilder().setCursor('p1').build(),
        sorted(fieldSort('id'))), document)

    then:
    compiled.usesPointInTime()
    compiled.body().sort == [[id: [order: 'asc']]]
    compiled.warnings() == ['no _shard_doc tiebreaker: pages are complete only if the sort ends with a unique field']
    !noShardDoc.config().shardDocTiebreaker()
    QueryCompilerConfig.standard().shardDocTiebreaker()
  }

  // ---- opt-in features ---------------------------------------------------------------------

  def "script sorts are rejected by default"() {
    when:
    compiler.compile(sorted(scriptSort(ScriptSort.newBuilder().setSource('1'))), document)

    then:
    def e = thrown(QueryCompileException)
    e.message.contains('Script sorts are disabled')
    e.message.contains('QueryCompilerConfig.allowScriptSort')
  }

  def "an allowed inline script sort compiles to Painless source"() {
    when:
    List sort = scriptsAllowed.compile(sorted(scriptSort(ScriptSort.newBuilder()
        .setSource("doc['size_bytes'].value").setSortType(ScriptSortType.NUMBER)
        .setOrder(SortingOrder.DESC))), document).body().sort

    then:
    sort == [[_script: [order: 'desc', type: 'number',
                        script: [lang: 'painless', source: "doc['size_bytes'].value"]]]]
  }

  def "an allowed stored script sort compiles to its id without a language"() {
    when:
    List sort = scriptsAllowed.compile(sorted(scriptSort(ScriptSort.newBuilder()
        .setId('by-size').setSortType(ScriptSortType.STRING))), document).body().sort

    then:
    sort == [[_script: [order: 'asc', type: 'string', script: [id: 'by-size']]]]
  }

  def "TEXT_REGEX is rejected by default"() {
    when:
    compiler.compile(where(fc('firm_id', TEXT_REGEX, str('f[0-9]+'))), document)

    then:
    def e = thrown(QueryCompileException)
    e.field == 'firm_id'
    e.message.contains('QueryCompilerConfig.allowRegex')
  }

  def "an allowed TEXT_REGEX with a literal prefix compiles to regexp"() {
    expect:
    regexAllowed.compileQuery(where(fc('firm_id', TEXT_REGEX, str('f[0-9]+|g.*'))), document)
        .bool.filter[0] == [regexp: [firm_id: [value: 'f[0-9]+|g.*']]]
  }

  @Unroll
  def "TEXT_REGEX '#pattern' needs allowLeadingWildcards too"() {
    when:
    regexAllowed.compile(where(fc('firm_id', TEXT_REGEX, str(pattern))), document)

    then:
    def e = thrown(QueryCompileException)
    e.message.contains('allowLeadingWildcards')

    when:
    def both = QueryCompiler.of(QueryCompilerConfig.standard().withAllowRegex(true)
        .withAllowLeadingWildcards(true))

    then:
    both.compileQuery(where(fc('firm_id', TEXT_REGEX, str(pattern))), document)
        .bool.filter[0] == [regexp: [firm_id: [value: pattern]]]

    where:
    pattern << ['.*foo', '.+foo', '.?foo', '[a-z]+', '(a|b)c', '~a', 'f.*|.*g', 'a?.*', 'a*.*',
                'a{0,1}.*', '\\.?.*', '\\w.*']
  }

  @Unroll
  def "'#pattern' starts with a literal: #expected"() {
    expect:
    ConditionCompiler.startsWithLiteral(pattern) == expected

    where:
    pattern       || expected
    'abc'         || true
    'f[0-9]+'     || true
    '\\.foo'      || true
    'a|b'         || true
    'a(b|.*)c'    || true
    'a[|]|b'      || true
    ''            || false
    'a|'          || false
    '.*foo'       || false
    '@'           || false
    '<1-5>'       || false
    '#'           || false
    '"quoted"'    || false
    'a|[bc]'      || false
    'a\\|b|(c)'   || false
    'a?.*'        || false
    'a*.*'        || false
    'a{0,1}.*'    || false
    '\\.?.*'      || false
    'a"("|.*'     || false
    'a"["|.*'     || false
    'a"x"'        || false
    '\\w.*'       || false
    '\\d+'        || false
    '\\S'         || false
    '\\'          || false
    '\\\\w'       || true
    'a+.*'        || true
    'ab?.*'       || true
  }

  // ---- page size ---------------------------------------------------------------------------

  def "size above maxPageSize is rejected with from/size"() {
    given:
    def small = QueryCompiler.of(QueryCompilerConfig.standard().withMaxPageSize(100))

    when:
    small.compile(paged(Pagination.newBuilder().setSize(101).build()), document)

    then:
    def e = thrown(QueryCompileException)
    e.message.contains('exceeds the maximum page size of 100')

    expect:
    small.compile(paged(Pagination.newBuilder().setSize(100).build()), document).body().size == 100
  }

  def "size above maxPageSize is rejected with a point in time"() {
    given:
    def small = QueryCompiler.of(QueryCompilerConfig.standard().withMaxPageSize(100))

    when:
    small.compile(paged(Pagination.newBuilder().setCursor('p1').setSize(101).build()), document)

    then:
    def e = thrown(QueryCompileException)
    e.message.contains('QueryCompilerConfig.maxPageSize')

    expect:
    small.compile(paged(Pagination.newBuilder().setCursor('p1').setSize(100).build()), document)
        .body().size == 100
  }

  def "the default maxPageSize is 10000 in both modes"() {
    when:
    compiler.compile(paged(Pagination.newBuilder().setCursor('p1').setSize(10_001).build()), document)

    then:
    thrown(QueryCompileException)

    when:
    compiler.compile(paged(Pagination.newBuilder().setSize(10_001).build()), document)

    then:
    thrown(QueryCompileException)

    expect:
    compiler.compile(paged(Pagination.newBuilder().setCursor('p1').setSize(10_000).build()), document)
        .body().size == 10_000
  }

  def "a default size above maxPageSize is rejected when the config is built"() {
    when:
    QueryCompilerConfig.standard().withDefaultSize(50).withMaxPageSize(20)

    then:
    def e = thrown(IllegalArgumentException)
    e.message == 'defaultSize 50 must not exceed maxPageSize 20'
  }

  // ---- config ------------------------------------------------------------------------------

  def "config copies and validation"() {
    given:
    def config = QueryCompilerConfig.standard()

    expect:
    config == QueryCompilerConfig.builder().build()
    config.trackTotalHits() == TrackTotalHits.exact()
    config.maxResultWindow() == 10000
    config.defaultSize() == 10
    config.defaultKeepAlive() == Duration.ofMinutes(1)
    config.groupNested()
    config.shardDocTiebreaker()
    !config.allowScriptSort()
    !config.allowRegex()
    config.maxPageSize() == QueryCompilerConfig.DEFAULT_MAX_PAGE_SIZE
    config.withAllowScriptSort(true).allowScriptSort()
    config.withAllowRegex(true).allowRegex()
    config.withMaxPageSize(70).maxPageSize() == 70
    config.withLenient(true).lenient()
    config.withAllowLeadingWildcards(true).allowLeadingWildcards()
    !config.withGroupNested(false).groupNested()
    config.withMaxResultWindow(5).maxResultWindow() == 5
    config.withDefaultSize(3).defaultSize() == 3
    config.withDefaultKeepAlive(Duration.ofSeconds(5)).defaultKeepAlive() == Duration.ofSeconds(5)
    config.withTrackTotalHits(TrackTotalHits.none()).trackTotalHits() == TrackTotalHits.none()
    config.withTrackTotalHits(TrackTotalHits.engineDefault()).trackTotalHits() == TrackTotalHits.engineDefault()
    !config.withShardDocTiebreaker(false).shardDocTiebreaker()
    QueryCompiler.of(config).config() == config
  }

  @Unroll
  def "config rejects #description"() {
    when:
    change(QueryCompilerConfig.standard())

    then:
    thrown(exception)

    where:
    description           | change                                            || exception
    'negative totals'     | { it.withTrackTotalHits(TrackTotalHits.upTo(-1)) } || IllegalArgumentException
    'zero window'         | { it.withMaxResultWindow(0) }                     || IllegalArgumentException
    'zero max page size'  | { it.withMaxPageSize(0) }                         || IllegalArgumentException
    'negative page cap'   | { it.withMaxPageSize(-1) }                        || IllegalArgumentException
    'default above cap'   | { it.withMaxPageSize(9) }                         || IllegalArgumentException
    'negative size'       | { it.withDefaultSize(-1) }                        || IllegalArgumentException
    'null keep-alive'     | { it.withDefaultKeepAlive(null) }                 || NullPointerException
    'null totals'         | { it.withTrackTotalHits(null) }                   || NullPointerException
    'zero keep-alive'     | { it.withDefaultKeepAlive(Duration.ZERO) }        || IllegalArgumentException
  }

  def "compiled searches copy warnings"() {
    given:
    def warnings = ['a']
    def compiled = new CompiledSearch([:], warnings, false)

    when:
    warnings << 'b'

    then:
    compiled.warnings() == ['a']
  }

  static SortingCriteria scriptSort(ScriptSort.Builder sort) {
    SortingCriteria.newBuilder().setScriptSort(sort).build()
  }

  static SortingCriteria fieldSort(String name) {
    SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder().setFieldName(name)).build()
  }
}
