package com.phatjam98.elasticprotos.query;

import static com.phatjam98.elasticprotos.query.NestedPathResolver.object;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.phatjam98.elasticprotos.mapping.FieldInfo;
import com.phatjam98.elasticprotos.mapping.FieldKind;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.proto.FieldBoost;
import com.phatjam98.elasticprotos.proto.FieldCondition;
import com.phatjam98.elasticprotos.proto.FlatValue;
import com.phatjam98.elasticprotos.proto.FullTextOperator;
import com.phatjam98.elasticprotos.proto.FullTextOptions;
import com.phatjam98.elasticprotos.proto.GeoBoundingBoxValue;
import com.phatjam98.elasticprotos.proto.GeoDistanceValue;
import com.phatjam98.elasticprotos.proto.GeoShape;
import com.phatjam98.elasticprotos.proto.LatLon;
import com.phatjam98.elasticprotos.proto.PrimitiveValue;
import com.phatjam98.elasticprotos.proto.RangeValue;
import com.phatjam98.elasticprotos.proto.SearchCondition;
import com.phatjam98.elasticprotos.proto.SearchOperationType;
import com.phatjam98.elasticprotos.query.NestedPathResolver.Clause;
import com.phatjam98.elasticprotos.query.NestedPathResolver.Occur;
import com.phatjam98.elasticprotos.util.Json;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/** Compiles the field conditions of one {@link SearchCondition} into a {@code bool} query. */
final class ConditionCompiler {

  private final QueryCompilerConfig config;
  private final IndexDefinition index;
  private final NestedPathResolver resolver;
  private final List<String> warnings;

  ConditionCompiler(QueryCompilerConfig config, IndexDefinition index, List<String> warnings) {
    this.config = config;
    this.index = index;
    this.resolver = new NestedPathResolver(index, config.groupNested());
    this.warnings = warnings;
  }

  /** Compiles one condition; a condition without usable predicates matches everything. */
  Map<String, Object> compile(SearchCondition condition) {
    List<Clause> clauses = new ArrayList<>();
    for (FieldCondition fieldCondition : condition.getFieldConditionList()) {
      Clause clause = compile(fieldCondition);
      if (clause != null) {
        clauses.add(clause);
      }
    }
    if (clauses.isEmpty()) {
      return object("match_all", new TreeMap<>());
    }
    return resolver.bool(clauses);
  }

  private Clause compile(FieldCondition condition) {
    SearchOperationType operation = condition.getOperation();
    if (operation == SearchOperationType.FULL_TEXT) {
      return fullText(condition);
    }
    String name = condition.getField();
    if (operation == SearchOperationType.SEARCH_UNSUPPORTED
        || operation == SearchOperationType.UNRECOGNIZED) {
      throw new QueryCompileException(
          "Unsupported operation " + operation + " on field '" + name + "'", name);
    }
    if (name.isEmpty()) {
      throw new QueryCompileException(
          "A field name is required for " + operation + "; only FULL_TEXT may leave it empty",
          name);
    }
    FieldInfo field = index.resolve(name).orElse(null);
    if (field == null) {
      field = flattenedKey(name, operation);
    }
    if (field == null) {
      unknown(name, operation.name());
      return null;
    }
    switch (operation) {
      case EQUALS:
        return equalsClause(condition, field);
      case EXISTS:
        return existsClause(condition, field);
      case TEXT_MATCHES:
        return matches(condition, field);
      case TEXT_CONTAINS:
        return contains(condition, field);
      case TEXT_PREFIX:
        return exactText(condition, field, "prefix", text(condition, field));
      case TEXT_SUFFIX:
        return suffix(condition, field);
      case TEXT_REGEX:
        return regex(condition, field);
      case NUM_RANGE:
        return range(condition, field);
      case ANY_IN:
        return anyIn(condition, field);
      case GEO_DISTANCE:
        return geoDistance(condition, field);
      case GEO_BOUNDING_BOX:
        return boundingBox(condition, field);
      case GEO_INTERSECTS:
        return geoShape(condition, field, "intersects");
      case GEO_CONTAINS:
        return geoShape(condition, field, "contains");
      case GEO_CONTAINED:
        return geoShape(condition, field, "within");
      default:
        throw new QueryCompileException(
            "Unsupported operation " + operation + " on field '" + name + "'", name);
    }
  }

  /**
   * Resolves a path below a {@code flattened} field, such as {@code labels.env} under a {@code
   * map<string, string> labels}. The engine indexes every key of a flattened field as a keyword
   * reachable by its dotted path, so such a path takes the exact-value operations: {@code EQUALS},
   * {@code ANY_IN}, {@code EXISTS} and {@code TEXT_PREFIX}.
   *
   * @return a field description for the key, or null when no flattened field encloses the path
   * @throws QueryCompileException for an operation keys do not support
   */
  private FieldInfo flattenedKey(String name, SearchOperationType operation) {
    for (int dot = name.lastIndexOf('.'); dot > 0; dot = name.lastIndexOf('.', dot - 1)) {
      FieldInfo root = index.fields().get(name.substring(0, dot));
      if (root == null || root.kind() != FieldKind.FLATTENED) {
        continue;
      }
      if (dot == name.length() - 1) {
        return null;
      }
      switch (operation) {
        case EQUALS:
        case ANY_IN:
        case EXISTS:
        case TEXT_PREFIX:
          String key = name.substring(dot + 1);
          return new FieldInfo(name, root.protoPath() + "." + key, FieldKind.FLATTENED,
              root.repeated(), false, root.boost(), root.field());
        default:
          throw new QueryCompileException(
              operation + " cannot be used on '" + name + "', a key of flattened field '"
                  + root.path() + "'; keys take EQUALS, ANY_IN, EXISTS and TEXT_PREFIX",
              name);
      }
    }
    return null;
  }

  private void unknown(String name, String what) {
    if (!config.lenient()) {
      FieldInfo values = index.typedMapValues(name).orElse(null);
      if (values != null) {
        String map = values.path().substring(0, values.path().length() - 2);
        throw new QueryCompileException(
            "Unknown field '" + name + "' in " + what + "; the values of map '" + map
                + "' in index " + index.alias() + " are " + valueType(values)
                + ", which does not map it",
            name);
      }
      throw new QueryCompileException(
          "Unknown field '" + name + "' in " + what + "; index " + index.alias()
              + " does not map it",
          name);
    }
    warnings.add("Skipped " + what + " on unknown field '" + name + "'");
  }

  /** Names the value type of a typed map: its message, or its mapped kind for other values. */
  private static String valueType(FieldInfo values) {
    return values.field().getJavaType() == FieldDescriptor.JavaType.MESSAGE
        ? values.field().getMessageType().getFullName()
        : values.kind().name();
  }

  private Clause clause(FieldCondition condition, FieldInfo field, Occur occur,
      Map<String, Object> query) {
    Occur actual = condition.getNegate() ? Occur.MUST_NOT : occur;
    return new Clause(actual, resolver.ancestors(field.path()), query);
  }

  // ---- EQUALS / EXISTS -------------------------------------------------------------------

  private Clause equalsClause(FieldCondition condition, FieldInfo field) {
    FlatValue value = condition.getValue();
    if (value.getKindCase() == FlatValue.KindCase.NULL_VALUE) {
      Map<String, Object> exists = exists(field);
      Map<String, Object> query = condition.getNegate()
          ? exists
          : object("bool", object("must_not", exists));
      return new Clause(Occur.FILTER, resolver.ancestors(field.path()), query);
    }
    requireExact(field, SearchOperationType.EQUALS);
    return clause(condition, field, Occur.FILTER,
        object("term", object(field.exactMatchPath(), ValueConversions.scalar(field, value))));
  }

  private Clause existsClause(FieldCondition condition, FieldInfo field) {
    return clause(condition, field, Occur.FILTER, exists(field));
  }

  /**
   * Returns the existence test for a field. A nested field itself cannot be tested with {@code
   * exists}, so "has at least one element" is a {@code nested} {@code match_all}.
   */
  private Map<String, Object> exists(FieldInfo field) {
    if (field.kind() == FieldKind.NESTED) {
      return resolver.wrapPaths(List.of(field.path()), object("match_all", new TreeMap<>()));
    }
    return object("exists", object("field", field.path()));
  }

  // ---- text ------------------------------------------------------------------------------

  private Clause matches(FieldCondition condition, FieldInfo field) {
    requireTextOrKeyword(field, SearchOperationType.TEXT_MATCHES);
    Occur occur = field.kind().isText() ? Occur.MUST : Occur.FILTER;
    return clause(condition, field, occur,
        object("match", object(field.path(), text(condition, field))));
  }

  private Clause contains(FieldCondition condition, FieldInfo field) {
    if (field.kind().isText()) {
      return clause(condition, field, Occur.MUST,
          object("match_phrase", object(field.path(), text(condition, field))));
    }
    requireTextOrKeyword(field, SearchOperationType.TEXT_CONTAINS);
    String pattern = leadingWildcard(field, SearchOperationType.TEXT_CONTAINS,
        "*" + escape(text(condition, field)) + "*");
    return exactText(condition, field, "wildcard", pattern);
  }

  private Clause suffix(FieldCondition condition, FieldInfo field) {
    requireTextOrKeyword(field, SearchOperationType.TEXT_SUFFIX);
    requireExact(field, SearchOperationType.TEXT_SUFFIX);
    String pattern = leadingWildcard(field, SearchOperationType.TEXT_SUFFIX,
        "*" + escape(text(condition, field)));
    return exactText(condition, field, "wildcard", pattern);
  }

  private Clause exactText(FieldCondition condition, FieldInfo field, String type, String value) {
    requireTextOrKeyword(field, condition.getOperation());
    requireExact(field, condition.getOperation());
    return clause(condition, field, Occur.FILTER,
        object(type, object(field.exactMatchPath(), object("value", value))));
  }

  /**
   * Compiles {@code TEXT_REGEX}, which must be enabled by {@link
   * QueryCompilerConfig#allowRegex()}. A pattern with an alternative that does not start with a
   * literal character, such as {@code .*foo}, {@code [a-z]+} or {@code (a|b)c}, can match from
   * any term and is a leading wildcard, so it also needs {@link
   * QueryCompilerConfig#allowLeadingWildcards()}.
   */
  private Clause regex(FieldCondition condition, FieldInfo field) {
    if (!config.allowRegex()) {
      throw new QueryCompileException(
          "TEXT_REGEX on '" + field.path() + "' is disabled: a regular expression can scan every"
              + " term of the field; enable QueryCompilerConfig.allowRegex to accept it",
          field.path());
    }
    String pattern = text(condition, field);
    if (!config.allowLeadingWildcards() && !startsWithLiteral(pattern)) {
      throw new QueryCompileException(
          "TEXT_REGEX pattern '" + pattern + "' on '" + field.path() + "' does not start with a"
              + " literal character, so it scans every term like a leading wildcard; start each"
              + " alternative with a literal prefix or enable"
              + " QueryCompilerConfig.allowLeadingWildcards",
          field.path());
    }
    return exactText(condition, field, "regexp", pattern);
  }

  /**
   * Returns whether every top-level alternative of a Lucene regular expression starts with a
   * literal character that must be present, so that the engine can seek to a prefix instead of
   * scanning every term.
   *
   * <p>The first atom of each alternative must be a plain or an escaped character, not followed by
   * {@code ?}, {@code *} or <code>{</code>, which would make it optional. An operator such as
   * {@code .}, {@code [}, {@code (}, {@code ~}, {@code <}, {@code #}, a quantifier, the any-string
   * operator or a predefined class ({@code \d}, {@code \s}, {@code \w} and their negations) is not
   * a literal. A pattern with a quoted string ({@code "}) is always refused, since quotes change
   * how the rest of the pattern is read.
   */
  static boolean startsWithLiteral(String pattern) {
    if (pattern.indexOf('"') >= 0) {
      return false;
    }
    int depth = 0;
    boolean inClass = false;
    boolean branchStart = true;
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (branchStart) {
        int atomEnd;
        if (c == '\\') {
          if (i + 1 >= pattern.length() || "dDsSwW".indexOf(pattern.charAt(i + 1)) >= 0) {
            return false;
          }
          atomEnd = i + 1;
        } else if (".[(~@<#*+?{|}])&".indexOf(c) >= 0) {
          return false;
        } else {
          atomEnd = i;
        }
        if (atomEnd + 1 < pattern.length() && "?*{".indexOf(pattern.charAt(atomEnd + 1)) >= 0) {
          return false;
        }
        branchStart = false;
      }
      if (c == '\\') {
        i++;
      } else if (inClass) {
        inClass = c != ']';
      } else if (c == '[') {
        inClass = true;
      } else if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (c == '|' && depth == 0) {
        branchStart = true;
      }
    }
    return !branchStart;
  }

  private String leadingWildcard(FieldInfo field, SearchOperationType operation, String pattern) {
    if (!config.allowLeadingWildcards()) {
      throw new QueryCompileException(
          operation + " on keyword field '" + field.path() + "' needs a leading wildcard, which "
              + "scans every term; enable QueryCompilerConfig.allowLeadingWildcards or map the "
              + "field as text",
          field.path());
    }
    return pattern;
  }

  private static String text(FieldCondition condition, FieldInfo field) {
    return ValueConversions.string(field, condition.getValue());
  }

  /** Escapes the wildcard metacharacters {@code *}, {@code ?} and {@code \}. */
  static String escape(String text) {
    StringBuilder escaped = new StringBuilder(text.length());
    for (char c : text.toCharArray()) {
      if (c == '*' || c == '?' || c == '\\') {
        escaped.append('\\');
      }
      escaped.append(c);
    }
    return escaped.toString();
  }

  private static void requireTextOrKeyword(FieldInfo field, SearchOperationType operation) {
    FieldKind kind = field.kind();
    if (!kind.isText() && kind != FieldKind.KEYWORD && kind != FieldKind.FLATTENED) {
      throw new QueryCompileException(
          operation + " needs a text or keyword field but '" + field.path() + "' is mapped as "
              + kind,
          field.path());
    }
  }

  private static void requireExact(FieldInfo field, SearchOperationType operation) {
    if (!field.supportsExactMatch()) {
      throw new QueryCompileException(
          operation + " needs exact values but '" + field.path() + "' is analyzed text without a "
              + "keyword sub-field; add [(elastic_field).keyword_subfield = TRUE] to the field "
              + "or use TEXT_MATCHES",
          field.path());
    }
    if (field.kind().isContainer() || field.kind() == FieldKind.GEO_POINT
        || field.kind() == FieldKind.GEO_SHAPE) {
      throw new QueryCompileException(
          operation + " cannot be used on '" + field.path() + "' mapped as " + field.kind(),
          field.path());
    }
  }

  // ---- range / terms ---------------------------------------------------------------------

  private Clause range(FieldCondition condition, FieldInfo field) {
    FieldKind kind = field.kind();
    if (!kind.isNumeric() && !kind.isDate()) {
      throw new QueryCompileException(
          "NUM_RANGE needs a numeric or date field but '" + field.path() + "' is mapped as "
              + kind,
          field.path());
    }
    FlatValue value = condition.getValue();
    if (value.getKindCase() != FlatValue.KindCase.RANGE_VALUE) {
      throw new QueryCompileException(
          "NUM_RANGE on '" + field.path() + "' needs range_value but got "
              + ValueConversions.kindName(value),
          field.path());
    }
    RangeValue range = value.getRangeValue();
    Map<String, Object> bounds = new TreeMap<>();
    bounds.put(range.getMinInclusive() ? "gte" : "gt",
        ValueConversions.number(kind, range.getMin()));
    bounds.put(range.getMaxInclusive() ? "lte" : "lt",
        ValueConversions.number(kind, range.getMax()));
    if (kind.isDate()) {
      bounds.put("format", "epoch_millis");
    }
    return clause(condition, field, Occur.FILTER, object("range", object(field.path(), bounds)));
  }

  private Clause anyIn(FieldCondition condition, FieldInfo field) {
    requireExact(field, SearchOperationType.ANY_IN);
    FlatValue value = condition.getValue();
    List<Object> terms = new ArrayList<>();
    if (value.getKindCase() == FlatValue.KindCase.LIST_VALUE) {
      for (PrimitiveValue item : value.getListValue().getValuesList()) {
        terms.add(ValueConversions.scalar(field, ValueConversions.of(item)));
      }
    } else {
      terms.add(ValueConversions.scalar(field, value));
    }
    if (terms.isEmpty()) {
      throw new QueryCompileException(
          "ANY_IN on '" + field.path() + "' needs at least one value", field.path());
    }
    return clause(condition, field, Occur.FILTER,
        object("terms", object(field.exactMatchPath(), terms)));
  }

  // ---- geo -------------------------------------------------------------------------------

  private Clause geoDistance(FieldCondition condition, FieldInfo field) {
    requireKind(field, FieldKind.GEO_POINT, SearchOperationType.GEO_DISTANCE);
    FlatValue value = condition.getValue();
    if (value.getKindCase() != FlatValue.KindCase.GEO_DISTANCE_VALUE
        || value.getGeoDistanceValue().getDistance().isEmpty()) {
      throw new QueryCompileException(
          "GEO_DISTANCE on '" + field.path() + "' needs geo_distance_value with a point and a "
              + "distance such as \"10km\", got " + ValueConversions.kindName(value),
          field.path());
    }
    GeoDistanceValue distance = value.getGeoDistanceValue();
    Map<String, Object> query = new TreeMap<>();
    query.put("distance", distance.getDistance());
    query.put("distance_type", distance.getDistanceType().name().toLowerCase(Locale.ROOT));
    query.put(field.path(), point(distance.getPoint()));
    ignoreUnmapped(query, field);
    return clause(condition, field, Occur.FILTER, object("geo_distance", query));
  }

  private Clause boundingBox(FieldCondition condition, FieldInfo field) {
    requireKind(field, FieldKind.GEO_POINT, SearchOperationType.GEO_BOUNDING_BOX);
    FlatValue value = condition.getValue();
    GeoBoundingBoxValue box = value.getGeoBoundingBoxValue();
    Map<String, Object> corners = new TreeMap<>();
    if (value.getKindCase() == FlatValue.KindCase.GEO_BOUNDING_BOX_VALUE
        && !box.getWkt().isEmpty()) {
      corners.put("wkt", box.getWkt());
    } else if (value.getKindCase() == FlatValue.KindCase.GEO_BOUNDING_BOX_VALUE
        && box.hasTopLeft() && box.hasBottomRight()) {
      corners.put("top_left", point(box.getTopLeft()));
      corners.put("bottom_right", point(box.getBottomRight()));
    } else {
      throw new QueryCompileException(
          "GEO_BOUNDING_BOX on '" + field.path() + "' needs geo_bounding_box_value with wkt or "
              + "both top_left and bottom_right, got " + ValueConversions.kindName(value),
          field.path());
    }
    Map<String, Object> query = object(field.path(), corners);
    ignoreUnmapped(query, field);
    return clause(condition, field, Occur.FILTER, object("geo_bounding_box", query));
  }

  private Clause geoShape(FieldCondition condition, FieldInfo field, String relation) {
    requireKind(field, FieldKind.GEO_SHAPE, condition.getOperation());
    FlatValue value = condition.getValue();
    Object shape;
    if (value.getKindCase() == FlatValue.KindCase.LAT_LON_VALUE) {
      Map<String, Object> point = new TreeMap<>();
      point.put("type", "Point");
      point.put("coordinates",
          List.of(value.getLatLonValue().getLon(), value.getLatLonValue().getLat()));
      shape = point;
    } else if (value.getKindCase() == FlatValue.KindCase.GEO_SHAPE_VALUE) {
      shape = shape(field, value.getGeoShapeValue());
    } else {
      throw new QueryCompileException(
          condition.getOperation() + " on '" + field.path() + "' needs geo_shape_value or "
              + "lat_lon_value, got " + ValueConversions.kindName(value),
          field.path());
    }
    Map<String, Object> spec = new TreeMap<>();
    spec.put("shape", shape);
    spec.put("relation", relation);
    Map<String, Object> query = object(field.path(), spec);
    ignoreUnmapped(query, field);
    return clause(condition, field, Occur.FILTER, object("geo_shape", query));
  }

  /**
   * Adds {@code ignore_unmapped} to a geo query on a key of a typed map: the key is mapped only
   * once a document uses it, and unlike term-level queries the geo queries fail on an unmapped
   * field.
   */
  private static void ignoreUnmapped(Map<String, Object> query, FieldInfo field) {
    if (field.isDynamic()) {
      query.put("ignore_unmapped", true);
    }
  }

  private static Object shape(FieldInfo field, GeoShape shape) {
    if (!shape.getGeojson().isEmpty()) {
      try {
        return Json.parseObject(shape.getGeojson());
      } catch (IllegalArgumentException e) {
        throw new QueryCompileException(
            "Field '" + field.path() + "' got geojson that is not a JSON object", field.path());
      }
    }
    if (!shape.getWkt().isEmpty()) {
      return shape.getWkt();
    }
    throw new QueryCompileException(
        "Field '" + field.path() + "' got a geo_shape_value with neither wkt nor geojson",
        field.path());
  }

  private static Map<String, Object> point(LatLon point) {
    Map<String, Object> map = new TreeMap<>();
    map.put("lat", point.getLat());
    map.put("lon", point.getLon());
    return map;
  }

  private static void requireKind(FieldInfo field, FieldKind kind,
      SearchOperationType operation) {
    if (field.kind() != kind) {
      throw new QueryCompileException(
          operation + " needs a " + kind + " field but '" + field.path() + "' is mapped as "
              + field.kind(),
          field.path());
    }
  }

  // ---- full text -------------------------------------------------------------------------

  private Clause fullText(FieldCondition condition) {
    FlatValue value = condition.getValue();
    if (value.getKindCase() != FlatValue.KindCase.STRING_VALUE
        || value.getStringValue().isEmpty()) {
      throw new QueryCompileException(
          "FULL_TEXT needs a non-empty string_value, got " + ValueConversions.kindName(value),
          condition.getField());
    }
    FullTextOptions options = condition.getFullText();
    Map<String, Float> fields = new LinkedHashMap<>();
    if (options.getFieldsCount() > 0) {
      for (FieldBoost boost : options.getFieldsList()) {
        if (fullTextField(boost.getField())) {
          fields.put(boost.getField(), boost.getBoost());
        }
      }
    } else if (!condition.getField().isEmpty()) {
      if (fullTextField(condition.getField())) {
        fields.put(condition.getField(), 0f);
      }
    } else {
      if (index.fullTextFields().isEmpty()) {
        throw new QueryCompileException(
            "FULL_TEXT without fields needs full-text fields, but index " + index.alias()
                + " has none; list fields in full_text.fields or map text fields",
            "");
      }
      fields.putAll(index.fullTextFields());
    }
    if (fields.isEmpty()) {
      warnings.add("Skipped FULL_TEXT: none of its fields are mapped");
      return null;
    }

    Map<String, List<String>> groups = new TreeMap<>();
    Map<String, List<String>> groupAncestors = new TreeMap<>();
    for (Map.Entry<String, Float> entry : fields.entrySet()) {
      String innermost = resolver.innermost(entry.getKey());
      String key = innermost == null ? "" : innermost;
      groups.computeIfAbsent(key, k -> new ArrayList<>())
          .add(entry.getKey() + boostSuffix(entry.getValue()));
      groupAncestors.putIfAbsent(key, resolver.ancestors(entry.getKey()));
    }
    Occur occur = condition.getNegate() ? Occur.MUST_NOT : Occur.MUST;
    if (groups.size() == 1) {
      String key = groups.keySet().iterator().next();
      return new Clause(occur, groupAncestors.get(key),
          multiMatch(value.getStringValue(), groups.get(key), options));
    }
    List<Object> should = new ArrayList<>();
    for (Map.Entry<String, List<String>> group : groups.entrySet()) {
      should.add(resolver.wrapPaths(groupAncestors.get(group.getKey()),
          multiMatch(value.getStringValue(), group.getValue(), options)));
    }
    Map<String, Object> bool = new TreeMap<>();
    bool.put("should", should);
    bool.put("minimum_should_match", 1);
    return new Clause(occur, List.of(), object("bool", bool));
  }

  /**
   * Checks a {@code FULL_TEXT} field: a declared field, a key of a typed map such as {@code
   * notes.summary}, or a typed map pattern such as {@code notes.*}, which the engine expands to the
   * keys it has mapped.
   */
  private boolean fullTextField(String name) {
    FieldInfo field = index.dynamicFields().get(name);
    if (field == null) {
      field = index.resolve(name).orElse(null);
    }
    if (field == null) {
      unknown(name, "FULL_TEXT");
      return false;
    }
    if (!field.kind().isText() && field.kind() != FieldKind.KEYWORD) {
      throw new QueryCompileException(
          "FULL_TEXT needs text or keyword fields but '" + name + "' is mapped as "
              + field.kind(),
          name);
    }
    return true;
  }

  private static Map<String, Object> multiMatch(String query, List<String> fields,
      FullTextOptions options) {
    Map<String, Object> match = new TreeMap<>();
    match.put("query", query);
    match.put("fields", fields);
    match.put("type", options.getType().name().toLowerCase(Locale.ROOT));
    if (options.getOperator() == FullTextOperator.OPERATOR_AND) {
      match.put("operator", "and");
    }
    if (!options.getFuzziness().isEmpty()) {
      match.put("fuzziness", options.getFuzziness());
    }
    if (options.getMinimumShouldMatchPct() > 0) {
      match.put("minimum_should_match", options.getMinimumShouldMatchPct() + "%");
    }
    return object("multi_match", match);
  }

  /** Formats {@code ^boost}; 0 and 1 mean the default and add nothing. */
  static String boostSuffix(float boost) {
    if (boost == 0f || boost == 1f) {
      return "";
    }
    return boost == Math.rint(boost) ? "^" + (long) boost : "^" + boost;
  }
}
