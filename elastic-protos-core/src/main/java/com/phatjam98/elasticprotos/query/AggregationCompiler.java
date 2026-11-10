package com.phatjam98.elasticprotos.query;

import static com.phatjam98.elasticprotos.query.NestedPathResolver.object;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.phatjam98.elasticprotos.mapping.FieldInfo;
import com.phatjam98.elasticprotos.mapping.FieldKind;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.proto.Aggregation;
import com.phatjam98.elasticprotos.proto.CalendarInterval;
import com.phatjam98.elasticprotos.proto.CompositeAggregation;
import com.phatjam98.elasticprotos.proto.CompositeDateHistogramSource;
import com.phatjam98.elasticprotos.proto.CompositeSource;
import com.phatjam98.elasticprotos.proto.DateBounds;
import com.phatjam98.elasticprotos.proto.DateHistogramAggregation;
import com.phatjam98.elasticprotos.proto.FiltersAggregation;
import com.phatjam98.elasticprotos.proto.HistogramAggregation;
import com.phatjam98.elasticprotos.proto.KeyedFilter;
import com.phatjam98.elasticprotos.proto.MetricType;
import com.phatjam98.elasticprotos.proto.PrimitiveValue;
import com.phatjam98.elasticprotos.proto.RangeAggregation;
import com.phatjam98.elasticprotos.proto.RangeBucket;
import com.phatjam98.elasticprotos.proto.SearchCondition;
import com.phatjam98.elasticprotos.proto.SortingOrder;
import com.phatjam98.elasticprotos.proto.TermsAggregation;
import com.phatjam98.elasticprotos.proto.TermsOrder;
import com.phatjam98.elasticprotos.proto.TermsOrderBy;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Compiles {@link Aggregation}s into the {@code aggs} object of a search body.
 *
 * <p>Every aggregation runs in a context: the nested paths enclosing the documents its parent
 * bucket holds (the root for top-level aggregations). When the field's nested paths differ from
 * the context, the aggregation is wrapped: a {@code reverse_nested} named {@code <name>__reverse}
 * climbs to the deepest common nested path (or the root), then one {@code nested} per level named
 * {@code <name>__nested}, {@code <name>__nested2}, ... descends to the field. The sub-aggregations
 * of a bucket aggregation run in the context of its field. A {@code filter} or {@code filters}
 * aggregation without a field runs in its parent's context; with a nested path as field it runs
 * on the nested objects of that path. A {@code composite} aggregation runs in the context of its
 * sources, which must share one.
 */
final class AggregationCompiler {

  /** Keys of the engines' aggregation and bucket objects, which a name must not shadow. */
  static final Set<String> RESERVED_NAMES = Set.of("key", "key_as_string", "doc_count",
      "buckets", "value", "value_as_string", "from", "to", "from_as_string", "to_as_string",
      "sum_other_doc_count", "doc_count_error_upper_bound", "meta");

  /** Separates a user name from a wrapper suffix; user names must not contain it. */
  static final String WRAPPER = "__";

  /** The engines' bucket count of a terms or composite aggregation without a size. */
  private static final int DEFAULT_BUCKETS = 10;

  private static final List<String> FORBIDDEN_CHARACTERS = List.of("[", "]", ">", ".");

  private static final Pattern FIXED_INTERVAL = Pattern.compile("[1-9][0-9]*(ms|s|m|h|d)");

  private final QueryCompilerConfig config;
  private final IndexDefinition index;
  private final List<String> warnings;

  AggregationCompiler(QueryCompilerConfig config, IndexDefinition index, List<String> warnings) {
    this.config = config;
    this.index = index;
    this.warnings = warnings;
  }

  /**
   * Compiles top-level aggregations.
   *
   * @param aggregations the aggregations of the criteria
   * @return the {@code aggs} object; empty when there is nothing to compute
   */
  Map<String, Object> compile(List<Aggregation> aggregations) {
    Map<String, Object> body = level(aggregations, List.of(), 0).body();
    if (config.maxAggregationBuckets() > 0) {
      checkBuckets(body, 1L, "");
    }
    return body;
  }

  /** The compiled aggregations of one level and the order paths of their names. */
  private record Level(Map<String, Object> body, Map<String, String> orderPaths) {}

  /** One compiled aggregation: its outermost name and body, and the path to its values. */
  private record Emitted(String name, Map<String, Object> body, String orderPath) {}

  private Level level(List<Aggregation> aggregations, List<String> context, int depth) {
    Set<String> names = new HashSet<>();
    Map<String, Object> body = new TreeMap<>();
    Map<String, String> orderPaths = new HashMap<>();
    for (Aggregation aggregation : aggregations) {
      checkName(aggregation.getName(), names);
      Emitted emitted = aggregation(aggregation, context, depth);
      if (emitted != null) {
        body.put(emitted.name(), emitted.body());
        orderPaths.put(aggregation.getName(), emitted.orderPath());
      }
    }
    return new Level(body, orderPaths);
  }

  private static void checkName(String name, Set<String> siblings) {
    if (name.isEmpty()) {
      throw new QueryCompileException("Every aggregation needs a name", null);
    }
    checkCharacters("Aggregation name '" + name + "'", name);
    if (RESERVED_NAMES.contains(name)) {
      throw new QueryCompileException("Aggregation name '" + name + "' is reserved: it is a key"
          + " of the engine's bucket objects (" + String.join(", ",
              RESERVED_NAMES.stream().sorted().toList()) + ")",
          null);
    }
    if (!siblings.add(name)) {
      throw new QueryCompileException(
          "Aggregation name '" + name + "' is used twice on the same level", null);
    }
  }

  /** The character rules of aggregation names, also applied to composite source names. */
  private static void checkCharacters(String subject, String name) {
    if (name.contains(WRAPPER)) {
      throw new QueryCompileException(subject + " must not contain '" + WRAPPER
          + "', which is reserved for the nested wrappers", null);
    }
    for (String forbidden : FORBIDDEN_CHARACTERS) {
      if (name.contains(forbidden)) {
        throw new QueryCompileException(subject + " must not contain '" + forbidden + "': the"
            + " engines reject '[', ']' and '>' in names, and '.' and '>' break the paths that"
            + " order terms by a metric", null);
      }
    }
    if (name.endsWith("_")) {
      throw new QueryCompileException(subject + " must not end with '_', so that the nested"
          + " wrapper suffixes can be told apart from it", null);
    }
  }

  private Emitted aggregation(Aggregation aggregation, List<String> context, int depth) {
    String name = aggregation.getName();
    Aggregation.KindCase kind = aggregation.getKindCase();
    if (kind == Aggregation.KindCase.KIND_NOT_SET) {
      throw new QueryCompileException("Aggregation '" + name + "' sets no kind: set terms,"
          + " date_histogram, histogram, range, metric, filter, filters or composite",
          aggregation.getField());
    }
    if (aggregation.getSubAggregationCount() > 0) {
      if (kind == Aggregation.KindCase.METRIC) {
        throw new QueryCompileException("Metric aggregation '" + name + "' cannot have"
            + " sub-aggregations", aggregation.getField());
      }
      if (depth + 1 >= config.maxAggregationDepth()) {
        throw new QueryCompileException("Aggregation '" + name + "' is on level " + (depth + 1)
            + " and cannot have sub-aggregations: at most " + config.maxAggregationDepth()
            + (config.maxAggregationDepth() == 1 ? " level is" : " levels are")
            + " allowed (QueryCompilerConfig.maxAggregationDepth)",
            aggregation.getField());
      }
    }
    switch (kind) {
      case FILTER:
      case FILTERS:
        return filter(aggregation, context, depth);
      case COMPOSITE:
        if (depth > 0) {
          throw new QueryCompileException("Composite aggregation '" + name + "' must be a"
              + " top-level aggregation: the engines accept it only at the top level or directly"
              + " under a nested aggregation", null);
        }
        return composite(aggregation, depth);
      default:
        return fieldAggregation(aggregation, context, depth);
    }
  }

  private Emitted fieldAggregation(Aggregation aggregation, List<String> context, int depth) {
    String name = aggregation.getName();
    final Aggregation.KindCase kind = aggregation.getKindCase();
    String path = aggregation.getField();
    if (path.isEmpty()) {
      throw new QueryCompileException("Aggregation '" + name + "' needs a field", null);
    }
    boolean flattenedKey = false;
    FieldInfo field = index.resolve(path).orElse(null);
    if (field == null) {
      FieldInfo root = FlattenedKeys.root(index, path);
      if (root != null) {
        field = FlattenedKeys.key(root, path);
        flattenedKey = true;
      }
    }
    if (field == null) {
      skipUnknown("aggregation '" + name + "'", path);
      return null;
    }
    List<String> fieldPaths = index.nestedAncestors(field.path());
    checkNotInTypedMap("Aggregation '" + name + "' is on '" + path + "'", fieldPaths, path);

    Map<String, Object> inner;
    switch (kind) {
      case TERMS:
        Level terms = level(aggregation.getSubAggregationList(), fieldPaths, depth + 1);
        inner = terms(name, aggregation.getTerms(), field, flattenedKey, terms,
            aggregation.getSubAggregationList());
        putSubs(inner, terms.body());
        break;
      case DATE_HISTOGRAM:
        requireNotKey("date_histogram aggregation '" + name + "'", field, flattenedKey);
        inner = object("date_histogram", dateHistogram(name, aggregation.getDateHistogram(),
            field));
        putSubs(inner, level(aggregation.getSubAggregationList(), fieldPaths, depth + 1).body());
        break;
      case HISTOGRAM:
        requireNotKey("histogram aggregation '" + name + "'", field, flattenedKey);
        inner = object("histogram", histogram(name, aggregation.getHistogram(), field));
        putSubs(inner, level(aggregation.getSubAggregationList(), fieldPaths, depth + 1).body());
        break;
      case RANGE:
        requireNotKey("range aggregation '" + name + "'", field, flattenedKey);
        inner = range(name, aggregation.getRange(), field);
        putSubs(inner, level(aggregation.getSubAggregationList(), fieldPaths, depth + 1).body());
        break;
      default:
        inner = metric(name, aggregation.getMetric().getType(), field, flattenedKey);
        break;
    }
    return wrap(name, context, fieldPaths, inner);
  }

  /** Throws for an unknown field in strict mode; records a warning in lenient mode. */
  private void skipUnknown(String what, String path) {
    if (!config.lenient()) {
      throw ConditionCompiler.unknownField(index, path, what);
    }
    warnings.add("Skipped " + what + " on unknown field '" + path + "'");
  }

  private void checkNotInTypedMap(String what, List<String> nestedPaths, String path) {
    for (String nested : nestedPaths) {
      if (index.resolve(nested).map(FieldInfo::isDynamic).orElse(false)) {
        throw new QueryCompileException(what + " inside nested path '" + nested + "' of a typed"
            + " map; the engines cannot aggregate nested paths that may be unmapped", path);
      }
    }
  }

  private static void putSubs(Map<String, Object> inner, Map<String, Object> subs) {
    if (!subs.isEmpty()) {
      inner.put("aggs", subs);
    }
  }

  /**
   * Wraps an aggregation so that it runs in the nested context of its field: {@code
   * reverse_nested} up to the deepest nested path shared with the context, then {@code nested}
   * down to the field.
   */
  private static Emitted wrap(String name, List<String> context, List<String> fieldPaths,
      Map<String, Object> inner) {
    int common = 0;
    while (common < context.size() && common < fieldPaths.size()
        && context.get(common).equals(fieldPaths.get(common))) {
      common++;
    }
    List<String> names = new ArrayList<>();
    List<Map<String, Object>> wrappers = new ArrayList<>();
    if (context.size() > common) {
      Map<String, Object> reverse = new TreeMap<>();
      if (common > 0) {
        reverse.put("path", fieldPaths.get(common - 1));
      }
      names.add(name + WRAPPER + "reverse");
      wrappers.add(object("reverse_nested", reverse));
    }
    for (int i = common; i < fieldPaths.size(); i++) {
      int level = i - common + 1;
      names.add(name + WRAPPER + "nested" + (level == 1 ? "" : String.valueOf(level)));
      wrappers.add(object("nested", object("path", fieldPaths.get(i))));
    }
    Map<String, Object> body = inner;
    String childName = name;
    for (int i = wrappers.size() - 1; i >= 0; i--) {
      Map<String, Object> wrapper = wrappers.get(i);
      wrapper.put("aggs", object(childName, body));
      body = wrapper;
      childName = names.get(i);
    }
    names.add(name);
    return new Emitted(childName, body, String.join(">", names));
  }

  // ---- bucket guard --------------------------------------------------------------------------

  /**
   * Refuses aggregation paths whose requested bucket counts multiply beyond the limit, over the
   * compiled aggregations, so that what lenient mode skipped does not count. The nested wrappers
   * count one bucket and are left out of the reported path.
   */
  private void checkBuckets(Map<String, Object> aggs, long product, String path) {
    for (Map.Entry<String, Object> entry : aggs.entrySet()) {
      Map<String, Object> aggregation = map(entry.getValue());
      boolean wrapper = aggregation.containsKey("nested")
          || aggregation.containsKey("reverse_nested");
      long buckets = Math.max(1L, requestedBuckets(aggregation));
      long total = product > Long.MAX_VALUE / buckets ? Long.MAX_VALUE : product * buckets;
      String here = wrapper ? path : path.isEmpty() ? entry.getKey() : path + ">" + entry.getKey();
      if (total > config.maxAggregationBuckets()) {
        throw new QueryCompileException("Aggregations '" + here + "' ask for up to " + total
            + " buckets (the product of their sizes), more than the maximum of "
            + config.maxAggregationBuckets() + " (QueryCompilerConfig.maxAggregationBuckets);"
            + " request fewer buckets", null);
      }
      checkBuckets(map(aggregation.get("aggs")), total, here);
    }
  }

  /** The bucket count a compiled aggregation asks for; one when it depends on the data. */
  private static long requestedBuckets(Map<String, Object> aggregation) {
    if (aggregation.get("terms") instanceof Map<?, ?> terms) {
      return terms.get("size") instanceof Number size ? size.longValue() : DEFAULT_BUCKETS;
    }
    if (aggregation.get("composite") instanceof Map<?, ?> composite) {
      return composite.get("size") instanceof Number size ? size.longValue() : DEFAULT_BUCKETS;
    }
    for (String range : List.of("range", "date_range")) {
      if (aggregation.get(range) instanceof Map<?, ?> body
          && body.get("ranges") instanceof List<?> ranges) {
        return ranges.size();
      }
    }
    if (aggregation.get("filters") instanceof Map<?, ?> filters) {
      return map(filters.get("filters")).size() + (filters.containsKey("other_bucket_key") ? 1 : 0);
    }
    return 1L;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : Map.of();
  }

  // ---- filter and filters --------------------------------------------------------------------

  private Emitted filter(Aggregation aggregation, List<String> context, int depth) {
    String name = aggregation.getName();
    boolean single = aggregation.getKindCase() == Aggregation.KindCase.FILTER;
    String what = (single ? "Filter" : "Filters") + " aggregation '" + name + "'";
    List<String> filterContext = context;
    String path = aggregation.getField();
    if (!path.isEmpty()) {
      FieldInfo field = index.resolve(path).orElse(null);
      if (field == null) {
        skipUnknown((single ? "filter" : "filters") + " aggregation '" + name + "'", path);
        return null;
      }
      if (field.kind() != FieldKind.NESTED) {
        throw new QueryCompileException(what + " takes no field, or a nested path to filter the"
            + " nested objects at that path, but '" + path + "' is mapped as " + field.kind(),
            path);
      }
      List<String> paths = new ArrayList<>(index.nestedAncestors(field.path()));
      paths.add(field.path());
      checkNotInTypedMap(what + " filters '" + path + "'", paths, path);
      filterContext = List.copyOf(paths);
    }
    ConditionCompiler conditions = new ConditionCompiler(config, index, warnings, filterContext,
        what.substring(0, 1).toLowerCase(Locale.ROOT) + what.substring(1));
    Map<String, Object> inner;
    if (single) {
      inner = object("filter", condition(what, aggregation.getFilter().getConditionList(),
          conditions));
    } else {
      inner = object("filters", filters(what, aggregation.getFilters(), conditions));
    }
    if (conditions.skipped() > 0) {
      // lenient: a dropped predicate would widen the bucket into a plausible but wrong count
      warnings.add("Skipped " + what.substring(0, 1).toLowerCase(Locale.ROOT) + what.substring(1)
          + ": its condition names an unknown field");
      return null;
    }
    putSubs(inner, level(aggregation.getSubAggregationList(), filterContext, depth + 1).body());
    return wrap(name, context, filterContext, inner);
  }

  /** Compiles the OR of conditions of a filter, or of one keyed filter. */
  private static Map<String, Object> condition(String what, List<SearchCondition> conditions,
      ConditionCompiler compiler) {
    if (conditions.isEmpty()) {
      throw new QueryCompileException(what + " needs a condition; a SearchCondition without"
          + " field conditions matches everything", null);
    }
    return compiler.compileAny(conditions);
  }

  private static Map<String, Object> filters(String what, FiltersAggregation filters,
      ConditionCompiler conditions) {
    if (filters.getFiltersCount() == 0) {
      throw new QueryCompileException(what + " needs at least one filter", null);
    }
    Map<String, Object> keyed = new TreeMap<>();
    for (KeyedFilter filter : filters.getFiltersList()) {
      if (filter.getKey().isEmpty()) {
        throw new QueryCompileException(what + " has a filter without a key", null);
      }
      if (keyed.containsKey(filter.getKey())) {
        throw new QueryCompileException(what + " uses the key '" + filter.getKey() + "' twice",
            null);
      }
      keyed.put(filter.getKey(), condition(what + " filter '" + filter.getKey() + "'",
          filter.getConditionList(), conditions));
    }
    Map<String, Object> body = new TreeMap<>();
    body.put("filters", keyed);
    if (!filters.getOtherBucketKey().isEmpty()) {
      if (keyed.containsKey(filters.getOtherBucketKey())) {
        throw new QueryCompileException(what + " uses other_bucket_key '"
            + filters.getOtherBucketKey() + "', which is also the key of a filter", null);
      }
      body.put("other_bucket_key", filters.getOtherBucketKey());
    }
    return body;
  }

  // ---- composite -----------------------------------------------------------------------------

  /** One compiled composite source and the nested paths of its field. */
  private record Source(Map<String, Object> body, List<String> nestedPaths) {}

  private Emitted composite(Aggregation aggregation, int depth) {
    String name = aggregation.getName();
    String what = "Composite aggregation '" + name + "'";
    if (!aggregation.getField().isEmpty()) {
      throw new QueryCompileException(what + " takes no field; its sources name their fields",
          aggregation.getField());
    }
    CompositeAggregation composite = aggregation.getComposite();
    if (composite.getSourcesCount() == 0) {
      throw new QueryCompileException(what + " needs at least one source", null);
    }
    List<Object> sources = new ArrayList<>();
    Set<String> sourceNames = new TreeSet<>();
    List<String> nestedPaths = null;
    String firstSource = null;
    for (CompositeSource source : composite.getSourcesList()) {
      if (source.getName().isEmpty()) {
        throw new QueryCompileException(what + " has a source without a name", source.getField());
      }
      checkCharacters("Source name '" + source.getName() + "' of " + what.substring(0, 1)
          .toLowerCase(Locale.ROOT) + what.substring(1), source.getName());
      if (!sourceNames.add(source.getName())) {
        throw new QueryCompileException(what + " uses the source name '" + source.getName()
            + "' twice", source.getField());
      }
      Source compiled = source(name, source);
      if (compiled == null) {
        return null;
      }
      if (nestedPaths == null) {
        nestedPaths = compiled.nestedPaths();
        firstSource = source.getName();
      } else if (!nestedPaths.equals(compiled.nestedPaths())) {
        throw new QueryCompileException(what + " has sources in different nested contexts: '"
            + firstSource + "' in " + context(nestedPaths) + " and '" + source.getName()
            + "' in " + context(compiled.nestedPaths()) + "; all sources must be inside the"
            + " same nested path", source.getField());
      }
      sources.add(object(source.getName(), compiled.body()));
    }
    Map<String, Object> body = new TreeMap<>();
    body.put("sources", sources);
    if (composite.getSize() < 0) {
      throw new QueryCompileException(what + " has a negative size: " + composite.getSize(),
          null);
    }
    if (composite.getSize() > config.maxTermsSize()) {
      throw new QueryCompileException(what + " asks for " + composite.getSize() + " buckets per"
          + " page, more than the maximum of " + config.maxTermsSize()
          + " (QueryCompilerConfig.maxTermsSize)", null);
    }
    if (composite.getSize() > 0) {
      body.put("size", composite.getSize());
    }
    if (composite.getAfterCount() > 0) {
      if (!composite.getAfterMap().keySet().equals(sourceNames)) {
        throw new QueryCompileException(what + " has an after key with " + new TreeSet<>(
            composite.getAfterMap().keySet()) + " but its sources are " + sourceNames
            + "; pass the after_key of the previous page", null);
      }
      Map<String, Object> after = new TreeMap<>();
      composite.getAfterMap().forEach((source, value) -> after.put(source, afterValue(value)));
      body.put("after", after);
    }
    Map<String, Object> inner = object("composite", body);
    putSubs(inner, level(aggregation.getSubAggregationList(), nestedPaths, depth + 1).body());
    return wrap(name, List.of(), nestedPaths, inner);
  }

  private static String context(List<String> nestedPaths) {
    return nestedPaths.isEmpty() ? "the root documents"
        : "nested path '" + nestedPaths.get(nestedPaths.size() - 1) + "'";
  }

  /** Compiles one source; returns null when its field is unknown in lenient mode. */
  private Source source(String name, CompositeSource source) {
    String sourceWhat = "source '" + source.getName() + "' of composite aggregation '" + name
        + "'";
    String path = source.getField();
    if (path.isEmpty()) {
      throw new QueryCompileException("The " + sourceWhat + " needs a field", null);
    }
    boolean flattenedKey = false;
    FieldInfo field = index.resolve(path).orElse(null);
    if (field == null) {
      FieldInfo root = FlattenedKeys.root(index, path);
      if (root != null) {
        field = FlattenedKeys.key(root, path);
        flattenedKey = true;
      }
    }
    if (field == null) {
      skipUnknown("composite aggregation '" + name + "'", path);
      return null;
    }
    List<String> nestedPaths = index.nestedAncestors(field.path());
    checkNotInTypedMap("The " + sourceWhat + " is on '" + path + "'", nestedPaths, path);
    Map<String, Object> body = new TreeMap<>();
    String kind;
    switch (source.getKindCase()) {
      case TERMS:
        requireExact("Terms " + sourceWhat, field, flattenedKey);
        body.put("field", field.exactMatchPath());
        kind = "terms";
        break;
      case DATE_HISTOGRAM:
        requireNotKey("Date histogram " + sourceWhat, field, flattenedKey);
        if (!field.kind().isDate()) {
          throw wrongKind("Date histogram " + sourceWhat, "a date", field);
        }
        CompositeDateHistogramSource histogram = source.getDateHistogram();
        body.put("field", field.path());
        interval(body, "Date histogram " + sourceWhat,
            histogram.getIntervalCase() == CompositeDateHistogramSource.IntervalCase
                .CALENDAR_INTERVAL ? histogram.getCalendarInterval() : null,
            histogram.getIntervalCase() == CompositeDateHistogramSource.IntervalCase
                .FIXED_INTERVAL ? histogram.getFixedInterval() : null,
            histogram.getTimeZone(), field);
        if (!histogram.getFormat().isEmpty()) {
          body.put("format", histogram.getFormat());
        }
        kind = "date_histogram";
        break;
      case HISTOGRAM:
        requireNotKey("Histogram " + sourceWhat, field, flattenedKey);
        if (!field.kind().isNumeric()) {
          throw wrongKind("Histogram " + sourceWhat, "a numeric", field);
        }
        double interval = source.getHistogram().getInterval();
        if (!(interval > 0) || Double.isInfinite(interval)) {
          throw new QueryCompileException("Histogram " + sourceWhat + " needs a positive,"
              + " finite interval, got " + interval, field.path());
        }
        body.put("field", field.path());
        body.put("interval", plain(interval));
        kind = "histogram";
        break;
      default:
        throw new QueryCompileException("The " + sourceWhat + " sets no kind: set terms,"
            + " date_histogram or histogram", path);
    }
    switch (source.getOrder()) {
      case ASC:
        body.put("order", "asc");
        break;
      case DESC:
        body.put("order", "desc");
        break;
      case ORDER_UNDEFINED:
        break;
      default:
        throw new QueryCompileException("The " + sourceWhat + " has an unknown order", path);
    }
    if (source.getMissingBucket()) {
      body.put("missing_bucket", true);
    }
    return new Source(object(kind, body), nestedPaths);
  }

  /** Converts an after-key value; a value without a kind is null (a missing bucket). */
  private static Object afterValue(PrimitiveValue value) {
    switch (value.getKindCase()) {
      case STRING_VALUE:
        return value.getStringValue();
      case BOOL_VALUE:
        return value.getBoolValue();
      case INT_VALUE:
        return value.getIntValue();
      case NUMBER_VALUE:
        return value.getNumberValue();
      default:
        return null;
    }
  }

  // ---- terms -------------------------------------------------------------------------------

  private Map<String, Object> terms(String name, TermsAggregation terms, FieldInfo field,
      boolean flattenedKey, Level subs, List<Aggregation> declared) {
    requireExact("terms aggregation '" + name + "'", field, flattenedKey);
    Map<String, Object> body = new TreeMap<>();
    body.put("field", field.exactMatchPath());
    if (terms.getSize() < 0) {
      throw new QueryCompileException("Terms aggregation '" + name + "' has a negative size: "
          + terms.getSize(), field.path());
    }
    if (terms.getSize() > config.maxTermsSize()) {
      throw new QueryCompileException("Terms aggregation '" + name + "' asks for "
          + terms.getSize() + " buckets, more than the maximum of " + config.maxTermsSize()
          + " (QueryCompilerConfig.maxTermsSize)", field.path());
    }
    if (terms.getSize() > 0) {
      body.put("size", terms.getSize());
    }
    if (terms.hasMinDocCount()) {
      body.put("min_doc_count", minDocCount(name, terms.getMinDocCount(), field));
    }
    if (terms.getMissing().getKindCase() != PrimitiveValue.KindCase.KIND_NOT_SET) {
      body.put("missing", ValueConversions.scalar(field, ValueConversions.of(terms.getMissing())));
    }
    List<Object> orders = new ArrayList<>();
    for (TermsOrder order : terms.getOrderList()) {
      Map<String, Object> compiled = order(name, order, field, subs, declared);
      if (compiled != null) {
        orders.add(compiled);
      }
    }
    if (orders.size() == 1) {
      body.put("order", orders.get(0));
    } else if (!orders.isEmpty()) {
      body.put("order", orders);
    }
    return object("terms", body);
  }

  private Map<String, Object> order(String name, TermsOrder order, FieldInfo field, Level subs,
      List<Aggregation> declared) {
    TermsOrderBy by = order.getBy();
    if (by == TermsOrderBy.UNRECOGNIZED) {
      throw new QueryCompileException(
          "Terms aggregation '" + name + "' has an unknown order", field.path());
    }
    if (by != TermsOrderBy.BY_METRIC && !order.getMetric().isEmpty()) {
      throw new QueryCompileException("Terms aggregation '" + name + "' names order.metric '"
          + order.getMetric() + "' but orders " + by + "; set by = BY_METRIC", field.path());
    }
    if (by == TermsOrderBy.ORDER_BY_DEFAULT) {
      if (order.getDirection() == SortingOrder.ORDER_UNDEFINED) {
        return null;
      }
      by = TermsOrderBy.BY_COUNT;
    }
    String key;
    switch (by) {
      case BY_COUNT:
        key = "_count";
        break;
      case BY_KEY:
        key = "_key";
        break;
      default:
        key = metricPath(name, order.getMetric(), field, subs, declared);
        if (key == null) {
          return null;
        }
        break;
    }
    String direction;
    switch (order.getDirection()) {
      case ASC:
        direction = "asc";
        break;
      case DESC:
        direction = "desc";
        break;
      case ORDER_UNDEFINED:
        direction = by == TermsOrderBy.BY_KEY ? "asc" : "desc";
        break;
      default:
        throw new QueryCompileException("Terms aggregation '" + name
            + "' has an unknown order direction", field.path());
    }
    return object(key, direction);
  }

  private String metricPath(String name, String metric, FieldInfo field, Level subs,
      List<Aggregation> declared) {
    if (metric.isEmpty()) {
      throw new QueryCompileException("Terms aggregation '" + name + "' orders BY_METRIC but"
          + " names no metric; set order.metric to a metric sub-aggregation", field.path());
    }
    Aggregation sub = declared.stream().filter(a -> a.getName().equals(metric)).findFirst()
        .orElse(null);
    if (sub == null) {
      throw new QueryCompileException("Terms aggregation '" + name + "' orders by '" + metric
          + "', which is not one of its sub-aggregations", field.path());
    }
    if (sub.getKindCase() != Aggregation.KindCase.METRIC) {
      throw new QueryCompileException("Terms aggregation '" + name + "' orders by '" + metric
          + "', which is a bucket aggregation; order by a metric sub-aggregation", field.path());
    }
    String path = subs.orderPaths().get(metric);
    if (path == null) {
      warnings.add("Terms aggregation '" + name + "' keeps the default order: its order metric '"
          + metric + "' was skipped");
    }
    return path;
  }

  // ---- histograms ----------------------------------------------------------------------------

  private Map<String, Object> dateHistogram(String name, DateHistogramAggregation histogram,
      FieldInfo field) {
    if (!field.kind().isDate()) {
      throw wrongKind("date_histogram aggregation '" + name + "'", "a date", field);
    }
    Map<String, Object> body = new TreeMap<>();
    body.put("field", field.path());
    interval(body, "Date histogram '" + name + "'",
        histogram.getIntervalCase() == DateHistogramAggregation.IntervalCase.CALENDAR_INTERVAL
            ? histogram.getCalendarInterval() : null,
        histogram.getIntervalCase() == DateHistogramAggregation.IntervalCase.FIXED_INTERVAL
            ? histogram.getFixedInterval() : null,
        histogram.getTimeZone(), field);
    if (!histogram.getFormat().isEmpty()) {
      body.put("format", histogram.getFormat());
    }
    if (histogram.hasMinDocCount()) {
      body.put("min_doc_count", minDocCount(name, histogram.getMinDocCount(), field));
    }
    if (histogram.hasExtendedBounds()) {
      Map<String, Object> bounds = bounds(name, histogram.getExtendedBounds(), field);
      if (!bounds.isEmpty()) {
        body.put("extended_bounds", bounds);
      }
    }
    return body;
  }

  /**
   * Puts the interval and time zone of a date histogram, or of a composite date histogram source,
   * into its body.
   *
   * @param calendar the calendar interval, or null when not chosen
   * @param fixed the fixed interval, or null when not chosen
   */
  private static void interval(Map<String, Object> body, String what, CalendarInterval calendar,
      String fixed, String timeZone, FieldInfo field) {
    if (calendar != null) {
      body.put("calendar_interval", calendarInterval(what, calendar, field));
    } else if (fixed != null) {
      if (!FIXED_INTERVAL.matcher(fixed).matches()) {
        throw new QueryCompileException(what + " has fixed_interval '" + fixed + "'; use a"
            + " positive whole number and ms, s, m, h or d, such as \"90m\"", field.path());
      }
      body.put("fixed_interval", fixed);
    } else {
      throw new QueryCompileException(what + " needs calendar_interval or fixed_interval",
          field.path());
    }
    if (!timeZone.isEmpty()) {
      try {
        ZoneId.of(timeZone);
      } catch (DateTimeException e) {
        throw new QueryCompileException(what + " has an invalid time_zone '" + timeZone + "': "
            + e.getMessage(), field.path());
      }
      body.put("time_zone", timeZone);
    }
  }

  private static String calendarInterval(String what, CalendarInterval calendar,
      FieldInfo field) {
    switch (calendar) {
      case MINUTE:
        return "1m";
      case HOUR:
        return "1h";
      case DAY:
        return "1d";
      case WEEK:
        return "1w";
      case MONTH:
        return "1M";
      case QUARTER:
        return "1q";
      case YEAR:
        return "1y";
      default:
        throw new QueryCompileException(what + " has no valid calendar_interval: "
            + calendar, field.path());
    }
  }

  private static Map<String, Object> bounds(String name, DateBounds bounds, FieldInfo field) {
    Map<String, Object> out = new TreeMap<>();
    try {
      if (bounds.hasMin()) {
        out.put("min", Timestamps.toMillis(Timestamps.checkValid(bounds.getMin())));
      }
      if (bounds.hasMax()) {
        out.put("max", Timestamps.toMillis(Timestamps.checkValid(bounds.getMax())));
      }
    } catch (IllegalArgumentException e) {
      throw new QueryCompileException("Date histogram '" + name + "' has an invalid"
          + " extended_bounds timestamp: " + e.getMessage(), field.path());
    }
    if (out.size() == 2 && (Long) out.get("min") > (Long) out.get("max")) {
      throw new QueryCompileException("Date histogram '" + name + "' has extended_bounds with"
          + " min after max", field.path());
    }
    return out;
  }

  private static Map<String, Object> histogram(String name, HistogramAggregation histogram,
      FieldInfo field) {
    if (!field.kind().isNumeric()) {
      throw wrongKind("histogram aggregation '" + name + "'", "a numeric", field);
    }
    double interval = histogram.getInterval();
    if (!(interval > 0) || Double.isInfinite(interval)) {
      throw new QueryCompileException("Histogram '" + name + "' needs a positive, finite"
          + " interval, got " + interval, field.path());
    }
    if (!Double.isFinite(histogram.getOffset())) {
      throw new QueryCompileException("Histogram '" + name + "' needs a finite offset",
          field.path());
    }
    Map<String, Object> body = new TreeMap<>();
    body.put("field", field.path());
    body.put("interval", plain(interval));
    if (histogram.getOffset() != 0) {
      body.put("offset", plain(histogram.getOffset()));
    }
    if (histogram.hasMinDocCount()) {
      body.put("min_doc_count", minDocCount(name, histogram.getMinDocCount(), field));
    }
    return body;
  }

  // ---- range ---------------------------------------------------------------------------------

  private static Map<String, Object> range(String name, RangeAggregation range, FieldInfo field) {
    FieldKind kind = field.kind();
    if (!kind.isNumeric() && !kind.isDate()) {
      throw wrongKind("range aggregation '" + name + "'", "a numeric or date", field);
    }
    if (range.getRangesCount() == 0) {
      throw new QueryCompileException("Range aggregation '" + name + "' needs at least one"
          + " bucket in ranges", field.path());
    }
    List<Object> buckets = new ArrayList<>();
    for (RangeBucket bucket : range.getRangesList()) {
      buckets.add(bucket(name, bucket, field));
    }
    Map<String, Object> body = new TreeMap<>();
    body.put("field", field.path());
    body.put("ranges", buckets);
    if (kind.isDate()) {
      body.put("format", "epoch_millis");
      return object("date_range", body);
    }
    return object("range", body);
  }

  private static Map<String, Object> bucket(String name, RangeBucket bucket, FieldInfo field) {
    Double from = bucket.getLowerCase() == RangeBucket.LowerCase.FROM_TIME
        ? millis(name, bucket.getFromTime(), field)
        : bucket.hasFrom() ? bucket.getFrom() : null;
    Double to = bucket.getUpperCase() == RangeBucket.UpperCase.TO_TIME
        ? millis(name, bucket.getToTime(), field)
        : bucket.hasTo() ? bucket.getTo() : null;
    if ((from != null && from.isNaN()) || (to != null && to.isNaN())) {
      throw new QueryCompileException("Range aggregation '" + name + "' has a NaN bound",
          field.path());
    }
    // Only the natural-side infinity means "unset", as for NUM_RANGE.
    boolean hasFrom = from != null && from != Double.NEGATIVE_INFINITY;
    boolean hasTo = to != null && to != Double.POSITIVE_INFINITY;
    if (!hasFrom && !hasTo) {
      throw new QueryCompileException("Range aggregation '" + name + "' has a bucket"
          + describe(bucket) + " without from or to", field.path());
    }
    if ((hasFrom && from.isInfinite()) || (hasTo && to.isInfinite())
        || (hasFrom && hasTo && from >= to)) {
      throw new QueryCompileException("Range aggregation '" + name + "' has an empty bucket"
          + describe(bucket) + ": from " + (hasFrom ? from : "-") + ", to "
          + (hasTo ? to : "-") + " (buckets are [from, to))", field.path());
    }
    Map<String, Object> out = new TreeMap<>();
    if (!bucket.getKey().isEmpty()) {
      out.put("key", bucket.getKey());
    }
    if (hasFrom) {
      out.put("from", ValueConversions.number(field.kind(), from));
    }
    if (hasTo) {
      out.put("to", ValueConversions.number(field.kind(), to));
    }
    return out;
  }

  /** Converts a Timestamp bound to epoch millis; only date fields take Timestamp bounds. */
  private static Double millis(String name, Timestamp time, FieldInfo field) {
    if (!field.kind().isDate()) {
      throw new QueryCompileException("Range aggregation '" + name + "' has a from_time or"
          + " to_time bound, but '" + field.path() + "' is mapped as " + field.kind()
          + "; Timestamp bounds are for date fields, use from / to", field.path());
    }
    try {
      return (double) Timestamps.toMillis(Timestamps.checkValid(time));
    } catch (IllegalArgumentException e) {
      throw new QueryCompileException("Range aggregation '" + name + "' has an invalid"
          + " timestamp bound: " + e.getMessage(), field.path());
    }
  }

  private static String describe(RangeBucket bucket) {
    return bucket.getKey().isEmpty() ? "" : " '" + bucket.getKey() + "'";
  }

  // ---- metrics -------------------------------------------------------------------------------

  private static Map<String, Object> metric(String name, MetricType metric, FieldInfo field,
      boolean flattenedKey) {
    String type;
    String path = field.path();
    FieldKind kind = field.kind();
    switch (metric) {
      case MIN:
      case MAX:
        if (!kind.isNumeric() && !kind.isDate()) {
          throw wrongKind(metric.name() + " aggregation '" + name + "'", "a numeric or date",
              field);
        }
        type = metric.name().toLowerCase(Locale.ROOT);
        break;
      case AVG:
      case SUM:
        if (!kind.isNumeric()) {
          throw wrongKind(metric.name() + " aggregation '" + name + "'", "a numeric", field);
        }
        type = metric.name().toLowerCase(Locale.ROOT);
        break;
      case VALUE_COUNT:
      case CARDINALITY:
        requireExact(metric.name() + " aggregation '" + name + "'", field, flattenedKey);
        type = metric.name().toLowerCase(Locale.ROOT);
        path = field.exactMatchPath();
        break;
      default:
        throw new QueryCompileException("Metric aggregation '" + name + "' has no valid metric: "
            + metric, field.path());
    }
    return object(type, object("field", path));
  }

  // ---- checks --------------------------------------------------------------------------------

  private static void requireExact(String subject, FieldInfo field, boolean flattenedKey) {
    FieldKind kind = field.kind();
    if (!field.supportsExactMatch()) {
      throw new QueryCompileException(subject + " needs exact values"
          + " but '" + field.path() + "' is analyzed text without a keyword sub-field; add"
          + " [(elastic_field).keyword_subfield = TRUE] to the field", field.path());
    }
    if (kind == FieldKind.FLATTENED && !flattenedKey) {
      throw new QueryCompileException(subject + " cannot use flattened"
          + " field '" + field.path() + "' as a whole; aggregate one of its keys, such as '"
          + field.path() + ".<key>'", field.path());
    }
    if (kind.isContainer() || kind == FieldKind.GEO_POINT || kind == FieldKind.GEO_SHAPE
        || kind == FieldKind.BINARY) {
      throw new QueryCompileException(subject + " cannot use '"
          + field.path() + "' mapped as " + kind, field.path());
    }
  }

  private static void requireNotKey(String subject, FieldInfo field, boolean flattenedKey) {
    if (flattenedKey) {
      throw new QueryCompileException(subject + " cannot use '"
          + field.path() + "', a key of a flattened field; keys take terms, value_count and"
          + " cardinality", field.path());
    }
  }

  private static QueryCompileException wrongKind(String subject, String needed,
      FieldInfo field) {
    return new QueryCompileException(subject + " needs " + needed
        + " field but '" + field.path() + "' is mapped as " + field.kind(), field.path());
  }

  private static int minDocCount(String name, int value, FieldInfo field) {
    if (value < 0) {
      throw new QueryCompileException("Aggregation '" + name + "' has a negative min_doc_count: "
          + value, field.path());
    }
    return value;
  }

  /** Returns a whole number as a Long so that it renders without a fraction. */
  private static Object plain(double number) {
    return number == Math.rint(number) && Math.abs(number) < 9.007199254740992E15
        ? (Object) (long) number
        : (Object) number;
  }
}
