package com.phatjam98.elasticprotos.query;

import static com.phatjam98.elasticprotos.query.NestedPathResolver.object;

import com.google.protobuf.util.Timestamps;
import com.phatjam98.elasticprotos.mapping.FieldInfo;
import com.phatjam98.elasticprotos.mapping.FieldKind;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.proto.Aggregation;
import com.phatjam98.elasticprotos.proto.DateBounds;
import com.phatjam98.elasticprotos.proto.DateHistogramAggregation;
import com.phatjam98.elasticprotos.proto.HistogramAggregation;
import com.phatjam98.elasticprotos.proto.MetricType;
import com.phatjam98.elasticprotos.proto.PrimitiveValue;
import com.phatjam98.elasticprotos.proto.RangeAggregation;
import com.phatjam98.elasticprotos.proto.RangeBucket;
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
import java.util.regex.Pattern;

/**
 * Compiles {@link Aggregation}s into the {@code aggs} object of a search body.
 *
 * <p>Every aggregation runs in a context: the nested paths enclosing the documents its parent
 * bucket holds (the root for top-level aggregations). When the field's nested paths differ from
 * the context, the aggregation is wrapped: a {@code reverse_nested} named {@code <name>__reverse}
 * climbs to the deepest common nested path (or the root), then one {@code nested} per level named
 * {@code <name>__nested}, {@code <name>__nested2}, ... descends to the field. The sub-aggregations
 * of a bucket aggregation run in the context of its field.
 */
final class AggregationCompiler {

  /** Keys of the engines' aggregation and bucket objects, which a name must not shadow. */
  static final Set<String> RESERVED_NAMES = Set.of("key", "key_as_string", "doc_count",
      "buckets", "value", "value_as_string", "from", "to", "from_as_string", "to_as_string",
      "sum_other_doc_count", "doc_count_error_upper_bound", "meta");

  /** Separates a user name from a wrapper suffix; user names must not contain it. */
  static final String WRAPPER = "__";

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
    return level(aggregations, List.of(), 0).body();
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
    if (name.contains(WRAPPER)) {
      throw new QueryCompileException("Aggregation name '" + name + "' must not contain '"
          + WRAPPER + "', which is reserved for the nested wrappers", null);
    }
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

  private Emitted aggregation(Aggregation aggregation, List<String> context, int depth) {
    String name = aggregation.getName();
    Aggregation.KindCase kind = aggregation.getKindCase();
    if (kind == Aggregation.KindCase.KIND_NOT_SET) {
      throw new QueryCompileException("Aggregation '" + name + "' sets no kind: set terms,"
          + " date_histogram, histogram, range or metric", aggregation.getField());
    }
    if (aggregation.getSubAggregationCount() > 0) {
      if (kind == Aggregation.KindCase.METRIC) {
        throw new QueryCompileException("Metric aggregation '" + name + "' cannot have"
            + " sub-aggregations", aggregation.getField());
      }
      if (depth > 0) {
        throw new QueryCompileException("Aggregation '" + name + "' is a sub-aggregation and"
            + " cannot have sub-aggregations of its own; only one level is supported",
            aggregation.getField());
      }
    }
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
      String what = "aggregation '" + name + "'";
      if (!config.lenient()) {
        throw ConditionCompiler.unknownField(index, path, what);
      }
      warnings.add("Skipped " + what + " on unknown field '" + path + "'");
      return null;
    }
    List<String> fieldPaths = index.nestedAncestors(field.path());
    for (String nested : fieldPaths) {
      if (index.resolve(nested).map(FieldInfo::isDynamic).orElse(false)) {
        throw new QueryCompileException("Aggregation '" + name + "' is on '" + path + "' inside"
            + " nested path '" + nested + "' of a typed map; the engines cannot aggregate nested"
            + " paths that may be unmapped", path);
      }
    }

    Map<String, Object> inner;
    switch (kind) {
      case TERMS:
        Level terms = level(aggregation.getSubAggregationList(), fieldPaths, depth + 1);
        inner = terms(name, aggregation.getTerms(), field, flattenedKey, terms,
            aggregation.getSubAggregationList());
        putSubs(inner, terms.body());
        break;
      case DATE_HISTOGRAM:
        requireNotKey(name, field, flattenedKey, "date_histogram");
        inner = object("date_histogram", dateHistogram(name, aggregation.getDateHistogram(),
            field));
        putSubs(inner, level(aggregation.getSubAggregationList(), fieldPaths, depth + 1).body());
        break;
      case HISTOGRAM:
        requireNotKey(name, field, flattenedKey, "histogram");
        inner = object("histogram", histogram(name, aggregation.getHistogram(), field));
        putSubs(inner, level(aggregation.getSubAggregationList(), fieldPaths, depth + 1).body());
        break;
      case RANGE:
        requireNotKey(name, field, flattenedKey, "range");
        inner = range(name, aggregation.getRange(), field);
        putSubs(inner, level(aggregation.getSubAggregationList(), fieldPaths, depth + 1).body());
        break;
      default:
        inner = metric(name, aggregation.getMetric(), field, flattenedKey);
        break;
    }
    return wrap(name, context, fieldPaths, inner);
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

  // ---- terms -------------------------------------------------------------------------------

  private Map<String, Object> terms(String name, TermsAggregation terms, FieldInfo field,
      boolean flattenedKey, Level subs, List<Aggregation> declared) {
    requireExact(name, field, flattenedKey, "terms");
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
    if (terms.hasOrder()) {
      Map<String, Object> order = order(name, terms.getOrder(), field, subs, declared);
      if (order != null) {
        body.put("order", order);
      }
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
      throw wrongKind(name, "date_histogram", "a date", field);
    }
    Map<String, Object> body = new TreeMap<>();
    body.put("field", field.path());
    switch (histogram.getIntervalCase()) {
      case CALENDAR_INTERVAL:
        body.put("calendar_interval", calendarInterval(name, histogram, field));
        break;
      case FIXED_INTERVAL:
        String fixed = histogram.getFixedInterval();
        if (!FIXED_INTERVAL.matcher(fixed).matches()) {
          throw new QueryCompileException("Date histogram '" + name + "' has fixed_interval '"
              + fixed + "'; use a positive whole number and ms, s, m, h or d, such as \"90m\"",
              field.path());
        }
        body.put("fixed_interval", fixed);
        break;
      default:
        throw new QueryCompileException("Date histogram '" + name + "' needs calendar_interval"
            + " or fixed_interval", field.path());
    }
    if (!histogram.getTimeZone().isEmpty()) {
      try {
        ZoneId.of(histogram.getTimeZone());
      } catch (DateTimeException e) {
        throw new QueryCompileException("Date histogram '" + name + "' has an invalid"
            + " time_zone '" + histogram.getTimeZone() + "': " + e.getMessage(), field.path());
      }
      body.put("time_zone", histogram.getTimeZone());
    }
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

  private static String calendarInterval(String name, DateHistogramAggregation histogram,
      FieldInfo field) {
    switch (histogram.getCalendarInterval()) {
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
        throw new QueryCompileException("Date histogram '" + name + "' has no valid"
            + " calendar_interval: " + histogram.getCalendarInterval(), field.path());
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
      throw wrongKind(name, "histogram", "a numeric", field);
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
      throw wrongKind(name, "range", "a numeric or date", field);
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
    if ((bucket.hasFrom() && Double.isNaN(bucket.getFrom()))
        || (bucket.hasTo() && Double.isNaN(bucket.getTo()))) {
      throw new QueryCompileException("Range aggregation '" + name + "' has a NaN bound",
          field.path());
    }
    // Only the natural-side infinity means "unset", as for NUM_RANGE.
    boolean hasFrom = bucket.hasFrom() && bucket.getFrom() != Double.NEGATIVE_INFINITY;
    boolean hasTo = bucket.hasTo() && bucket.getTo() != Double.POSITIVE_INFINITY;
    if (!hasFrom && !hasTo) {
      throw new QueryCompileException("Range aggregation '" + name + "' has a bucket"
          + describe(bucket) + " without from or to", field.path());
    }
    if ((hasFrom && Double.isInfinite(bucket.getFrom()))
        || (hasTo && Double.isInfinite(bucket.getTo()))
        || (hasFrom && hasTo && bucket.getFrom() >= bucket.getTo())) {
      throw new QueryCompileException("Range aggregation '" + name + "' has an empty bucket"
          + describe(bucket) + ": from " + (hasFrom ? bucket.getFrom() : "-") + ", to "
          + (hasTo ? bucket.getTo() : "-") + " (buckets are [from, to))", field.path());
    }
    Map<String, Object> out = new TreeMap<>();
    if (!bucket.getKey().isEmpty()) {
      out.put("key", bucket.getKey());
    }
    if (hasFrom) {
      out.put("from", ValueConversions.number(field.kind(), bucket.getFrom()));
    }
    if (hasTo) {
      out.put("to", ValueConversions.number(field.kind(), bucket.getTo()));
    }
    return out;
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
          throw wrongKind(name, metric.name(), "a numeric or date", field);
        }
        type = metric.name().toLowerCase(Locale.ROOT);
        break;
      case AVG:
      case SUM:
        if (!kind.isNumeric()) {
          throw wrongKind(name, metric.name(), "a numeric", field);
        }
        type = metric.name().toLowerCase(Locale.ROOT);
        break;
      case VALUE_COUNT:
      case CARDINALITY:
        requireExact(name, field, flattenedKey, metric.name());
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

  private static void requireExact(String name, FieldInfo field, boolean flattenedKey,
      String what) {
    FieldKind kind = field.kind();
    if (!field.supportsExactMatch()) {
      throw new QueryCompileException(what + " aggregation '" + name + "' needs exact values"
          + " but '" + field.path() + "' is analyzed text without a keyword sub-field; add"
          + " [(elastic_field).keyword_subfield = TRUE] to the field", field.path());
    }
    if (kind == FieldKind.FLATTENED && !flattenedKey) {
      throw new QueryCompileException(what + " aggregation '" + name + "' cannot use flattened"
          + " field '" + field.path() + "' as a whole; aggregate one of its keys, such as '"
          + field.path() + ".<key>'", field.path());
    }
    if (kind.isContainer() || kind == FieldKind.GEO_POINT || kind == FieldKind.GEO_SHAPE
        || kind == FieldKind.BINARY) {
      throw new QueryCompileException(what + " aggregation '" + name + "' cannot use '"
          + field.path() + "' mapped as " + kind, field.path());
    }
  }

  private static void requireNotKey(String name, FieldInfo field, boolean flattenedKey,
      String what) {
    if (flattenedKey) {
      throw new QueryCompileException(what + " aggregation '" + name + "' cannot use '"
          + field.path() + "', a key of a flattened field; keys take terms, value_count and"
          + " cardinality", field.path());
    }
  }

  private static QueryCompileException wrongKind(String name, String what, String needed,
      FieldInfo field) {
    return new QueryCompileException(what + " aggregation '" + name + "' needs " + needed
        + " field but '" + field.path() + "' is mapped as " + field.kind(), field.path());
  }

  private static long minDocCount(String name, long value, FieldInfo field) {
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
