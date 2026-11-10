package com.phatjam98.elasticprotos.testkit;

import com.phatjam98.elasticprotos.engine.EngineException;
import com.phatjam98.elasticprotos.engine.EngineException.Kind;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The aggregations of {@link InMemorySearchEngine}: terms, single-value metrics, nested and
 * reverse_nested, over plain document maps, with keys typed by the index mappings.
 */
final class InMemoryAggregations {

  /** The engines' {@code key_as_string} / {@code value_as_string} form of dates. */
  private static final DateTimeFormatter ISO_MILLIS =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

  private final Map<String, String> types;

  /**
   * Creates the aggregations of one search.
   *
   * @param types the mapped type of every field path, from {@link #fieldTypes(Map)}
   */
  InMemoryAggregations(Map<String, String> types) {
    this.types = types;
  }

  /**
   * One object aggregations run over: a root document, or an element of a nested array.
   *
   * @param node the object
   * @param path its nested path; empty for a root document
   * @param parent the enclosing object, or null for a root document
   */
  record Scope(Map<String, Object> node, String path, Scope parent) {}

  /** Returns the mapped type of every field path of a mapping, sub-fields included. */
  static Map<String, String> fieldTypes(Map<String, Object> mappings) {
    Map<String, String> out = new TreeMap<>();
    collectTypes(map(mappings.get("properties")), "", out);
    return out;
  }

  private static void collectTypes(Map<String, Object> properties, String prefix,
      Map<String, String> out) {
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String path = prefix + entry.getKey();
      Map<String, Object> property = map(entry.getValue());
      out.put(path, String.valueOf(property.getOrDefault("type", "object")));
      for (Map.Entry<String, Object> sub : map(property.get("fields")).entrySet()) {
        out.put(path + "." + sub.getKey(), String.valueOf(map(sub.getValue()).get("type")));
      }
      collectTypes(map(property.get("properties")), path + ".", out);
    }
  }

  /** Runs the aggregations of one level over a set of objects. */
  Map<String, Object> run(Map<String, Object> aggs, List<Scope> scopes) {
    Map<String, Object> out = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : aggs.entrySet()) {
      out.put(entry.getKey(), one(entry.getKey(), map(entry.getValue()), scopes));
    }
    return out;
  }

  private Map<String, Object> one(String name, Map<String, Object> definition,
      List<Scope> scopes) {
    Map<String, Object> subs = map(definition.containsKey("aggs") ? definition.get("aggs")
        : definition.get("aggregations"));
    String type = null;
    for (String key : definition.keySet()) {
      if (!key.equals("aggs") && !key.equals("aggregations") && !key.equals("meta")) {
        type = key;
      }
    }
    if (type == null) {
      throw bad("aggregation [" + name + "] has no type");
    }
    Map<String, Object> body = map(definition.get(type));
    switch (type) {
      case "terms":
        return terms(body, subs, scopes);
      case "nested":
        return nested(String.valueOf(body.get("path")), subs, scopes);
      case "reverse_nested":
        return reverseNested(body.get("path") == null ? "" : String.valueOf(body.get("path")),
            subs, scopes);
      case "min":
      case "max":
      case "avg":
      case "sum":
      case "value_count":
      case "cardinality":
        if (!subs.isEmpty()) {
          throw bad("metric aggregation [" + name + "] cannot have sub-aggregations");
        }
        return metric(type, String.valueOf(body.get("field")), scopes);
      default:
        throw bad("the in-memory engine supports only the terms, min, max, avg, sum, value_count,"
            + " cardinality, nested and reverse_nested aggregations, not [" + type + "] in ["
            + name + "]");
    }
  }

  // ---- buckets -------------------------------------------------------------------------------

  private Map<String, Object> terms(Map<String, Object> body, Map<String, Object> subs,
      List<Scope> scopes) {
    String field = String.valueOf(body.get("field"));
    String type = types.get(field);
    Map<Object, List<Scope>> groups = new LinkedHashMap<>();
    for (Scope scope : scopes) {
      Set<Object> values = new LinkedHashSet<>();
      for (Object value : values(scope, field)) {
        values.add(typedKey(value, type));
      }
      if (values.isEmpty() && body.containsKey("missing")) {
        values.add(typedKey(body.get("missing"), type));
      }
      for (Object value : values) {
        groups.computeIfAbsent(value, k -> new ArrayList<>()).add(scope);
      }
    }
    long minDocCount = number(body.get("min_doc_count"), 1);
    List<Map<String, Object>> buckets = new ArrayList<>();
    for (Map.Entry<Object, List<Scope>> group : groups.entrySet()) {
      if (group.getValue().size() < minDocCount) {
        continue;
      }
      Map<String, Object> bucket = new LinkedHashMap<>();
      bucket.put("key", group.getKey());
      String keyAsString = keyAsString(group.getKey(), type);
      if (keyAsString != null) {
        bucket.put("key_as_string", keyAsString);
      }
      bucket.put("doc_count", (long) group.getValue().size());
      bucket.putAll(run(subs, group.getValue()));
      buckets.add(bucket);
    }
    buckets.sort(order(body.get("order")));
    int size = (int) number(body.get("size"), 10);
    long other = 0;
    for (int i = size; i < buckets.size(); i++) {
      other += (Long) buckets.get(i).get("doc_count");
    }
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("doc_count_error_upper_bound", 0L);
    out.put("sum_other_doc_count", other);
    out.put("buckets", new ArrayList<>(buckets.subList(0, Math.min(size, buckets.size()))));
    return out;
  }

  /** Typed bucket key: 1/0 for booleans, epoch millis for dates, Long for integral numbers. */
  private static Object typedKey(Object value, String type) {
    if ("boolean".equals(type) || value instanceof Boolean) {
      return Boolean.parseBoolean(String.valueOf(value)) ? 1L : 0L;
    }
    if (type != null && type.startsWith("date") && !(value instanceof Number)) {
      try {
        return Instant.parse(String.valueOf(value)).toEpochMilli();
      } catch (DateTimeParseException e) {
        throw bad("failed to parse date field [" + value + "]");
      }
    }
    return key(value);
  }

  private static String keyAsString(Object key, String type) {
    if ("boolean".equals(type)) {
      return Long.valueOf(1L).equals(key) ? "true" : "false";
    }
    if (type != null && type.startsWith("date") && key instanceof Number number) {
      return ISO_MILLIS.format(Instant.ofEpochMilli(number.longValue()));
    }
    return null;
  }

  private static Comparator<Map<String, Object>> order(Object order) {
    Comparator<Map<String, Object>> byKey = (a, b) -> compare(a.get("key"), b.get("key"));
    Comparator<Map<String, Object>> byCount =
        Comparator.comparing(bucket -> (Long) bucket.get("doc_count"));
    List<Map<String, Object>> criteria = new ArrayList<>();
    if (order instanceof List<?> list) {
      list.forEach(item -> criteria.add(map(item)));
    } else if (order != null) {
      criteria.add(map(order));
    }
    if (criteria.isEmpty()) {
      return byCount.reversed().thenComparing(byKey);
    }
    Comparator<Map<String, Object>> combined = null;
    for (Map<String, Object> criterion : criteria) {
      if (criterion.size() != 1) {
        throw bad("each terms order needs exactly one key, not " + criterion);
      }
      Map.Entry<String, Object> entry = criterion.entrySet().iterator().next();
      Comparator<Map<String, Object>> next;
      if (entry.getKey().equals("_key")) {
        next = byKey;
      } else if (entry.getKey().equals("_count")) {
        next = byCount;
      } else {
        String[] path = entry.getKey().split(">");
        next = (a, b) -> compare(metricValue(a, path), metricValue(b, path));
      }
      if ("desc".equals(entry.getValue())) {
        next = next.reversed();
      }
      combined = combined == null ? next : combined.thenComparing(next);
    }
    return combined.thenComparing(byKey);
  }

  private static Object metricValue(Map<String, Object> bucket, String[] path) {
    Map<String, Object> node = bucket;
    for (String step : path) {
      node = map(node.get(step));
    }
    Object value = node.get("value");
    return value == null ? Double.NEGATIVE_INFINITY : value;
  }

  private Map<String, Object> nested(String path, Map<String, Object> subs,
      List<Scope> scopes) {
    List<Scope> children = new ArrayList<>();
    for (Scope scope : scopes) {
      String relative = relative(scope, path);
      List<Object> found = new ArrayList<>();
      collect(scope.node(), relative.split("\\."), 0, found);
      for (Object child : found) {
        if (child instanceof Map) {
          children.add(new Scope(map(child), path, scope));
        }
      }
    }
    return singleBucket(children, subs);
  }

  private Map<String, Object> reverseNested(String path, Map<String, Object> subs,
      List<Scope> scopes) {
    Map<Scope, Boolean> seen = new IdentityHashMap<>();
    List<Scope> parents = new ArrayList<>();
    for (Scope scope : scopes) {
      Scope parent = scope;
      while (parent != null && !parent.path().equals(path)) {
        parent = parent.parent();
      }
      if (parent == null) {
        throw bad("reverse_nested path [" + path + "] does not enclose [" + scope.path() + "]");
      }
      if (seen.put(parent, Boolean.TRUE) == null) {
        parents.add(parent);
      }
    }
    return singleBucket(parents, subs);
  }

  private Map<String, Object> singleBucket(List<Scope> scopes, Map<String, Object> subs) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("doc_count", (long) scopes.size());
    out.putAll(run(subs, scopes));
    return out;
  }

  // ---- metrics -------------------------------------------------------------------------------

  private Map<String, Object> metric(String type, String field, List<Scope> scopes) {
    List<Object> all = new ArrayList<>();
    for (Scope scope : scopes) {
      all.addAll(values(scope, field));
    }
    Map<String, Object> out = new LinkedHashMap<>();
    if (type.equals("value_count")) {
      out.put("value", (long) all.size());
      return out;
    }
    if (type.equals("cardinality")) {
      Set<Object> distinct = new LinkedHashSet<>();
      for (Object value : all) {
        distinct.add(key(value));
      }
      out.put("value", (long) distinct.size());
      return out;
    }
    boolean dates = false;
    List<Double> numbers = new ArrayList<>();
    for (Object value : all) {
      if (value instanceof Number number) {
        numbers.add(number.doubleValue());
      } else {
        try {
          numbers.add((double) Instant.parse(String.valueOf(value)).toEpochMilli());
          dates = true;
        } catch (DateTimeParseException e) {
          throw bad("field [" + field + "] of type [keyword] is not supported for aggregation ["
              + type + "]");
        }
      }
    }
    Double value;
    switch (type) {
      case "min":
        value = numbers.isEmpty() ? null : Collections.min(numbers);
        break;
      case "max":
        value = numbers.isEmpty() ? null : Collections.max(numbers);
        break;
      case "sum":
        value = numbers.stream().mapToDouble(Double::doubleValue).sum();
        break;
      default:
        value = numbers.isEmpty() ? null
            : numbers.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
        break;
    }
    out.put("value", value);
    if (dates && value != null) {
      out.put("value_as_string", ISO_MILLIS.format(Instant.ofEpochMilli(value.longValue())));
    }
    return out;
  }

  // ---- helpers -------------------------------------------------------------------------------

  /**
   * Returns the values of a field below an object; the field path is absolute. A sub-field such as
   * {@code title.keyword} reads its parent's value, as the engine indexes it from that.
   */
  private List<Object> values(Scope scope, String field) {
    String source = field;
    int dot = field.lastIndexOf('.');
    if (dot > 0 && isSubField(field.substring(0, dot), field)) {
      source = field.substring(0, dot);
    }
    List<Object> out = new ArrayList<>();
    collect(scope.node(), relative(scope, source).split("\\."), 0, out);
    return out;
  }

  /** Whether a path is a multi-field of its parent, as opposed to an object property. */
  private boolean isSubField(String parent, String field) {
    String parentType = types.get(parent);
    return parentType != null && !parentType.equals("object") && !parentType.equals("nested")
        && types.containsKey(field);
  }

  private static String relative(Scope scope, String field) {
    if (scope.path().isEmpty()) {
      return field;
    }
    if (!field.startsWith(scope.path() + ".")) {
      throw bad("field [" + field + "] is not inside nested path [" + scope.path() + "]");
    }
    return field.substring(scope.path().length() + 1);
  }

  /** Collects the values at a dotted path, walking into arrays. */
  private static void collect(Object node, String[] path, int depth, List<Object> out) {
    if (node instanceof List) {
      for (Object item : (List<?>) node) {
        collect(item, path, depth, out);
      }
    } else if (depth == path.length) {
      if (node != null) {
        out.add(node);
      }
    } else if (node instanceof Map) {
      collect(((Map<?, ?>) node).get(path[depth]), path, depth + 1, out);
    }
  }

  /** Normalizes a value into a bucket key: integral numbers become Long. */
  private static Object key(Object value) {
    if (value instanceof Integer || value instanceof Long || value instanceof Short) {
      return ((Number) value).longValue();
    }
    if (value instanceof Number number) {
      double d = number.doubleValue();
      return d == Math.rint(d) && !Double.isInfinite(d) ? (Object) (long) d : (Object) d;
    }
    return value;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static int compare(Object a, Object b) {
    if (a instanceof Number x && b instanceof Number y) {
      return Double.compare(x.doubleValue(), y.doubleValue());
    }
    if (a instanceof Comparable && a.getClass().equals(b.getClass())) {
      return ((Comparable) a).compareTo(b);
    }
    return String.valueOf(a).compareTo(String.valueOf(b));
  }

  private static long number(Object value, long fallback) {
    return value instanceof Number number ? number.longValue() : fallback;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : new TreeMap<>();
  }

  private static EngineException bad(String message) {
    return new EngineException(Kind.BAD_REQUEST, 400, message);
  }
}
