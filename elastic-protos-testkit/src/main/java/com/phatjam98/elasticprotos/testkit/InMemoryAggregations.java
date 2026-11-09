package com.phatjam98.elasticprotos.testkit;

import com.phatjam98.elasticprotos.engine.EngineException;
import com.phatjam98.elasticprotos.engine.EngineException.Kind;
import java.time.Instant;
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
 * reverse_nested, over plain document maps.
 */
final class InMemoryAggregations {

  private InMemoryAggregations() {}

  /**
   * One object aggregations run over: a root document, or an element of a nested array.
   *
   * @param node the object
   * @param path its nested path; empty for a root document
   * @param parent the enclosing object, or null for a root document
   */
  record Scope(Map<String, Object> node, String path, Scope parent) {}

  /** Runs the aggregations of one level over a set of objects. */
  static Map<String, Object> run(Map<String, Object> aggs, List<Scope> scopes) {
    Map<String, Object> out = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : aggs.entrySet()) {
      out.put(entry.getKey(), one(entry.getKey(), map(entry.getValue()), scopes));
    }
    return out;
  }

  private static Map<String, Object> one(String name, Map<String, Object> definition,
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

  private static Map<String, Object> terms(Map<String, Object> body, Map<String, Object> subs,
      List<Scope> scopes) {
    String field = String.valueOf(body.get("field"));
    Map<Object, List<Scope>> groups = new LinkedHashMap<>();
    for (Scope scope : scopes) {
      Set<Object> values = new LinkedHashSet<>(values(scope, field));
      if (values.isEmpty() && body.containsKey("missing")) {
        values.add(body.get("missing"));
      }
      for (Object value : values) {
        groups.computeIfAbsent(key(value), k -> new ArrayList<>()).add(scope);
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
      bucket.put("doc_count", (long) group.getValue().size());
      bucket.putAll(run(subs, group.getValue()));
      buckets.add(bucket);
    }
    buckets.sort(order(map(body.get("order"))));
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

  private static Comparator<Map<String, Object>> order(Map<String, Object> order) {
    Comparator<Map<String, Object>> byKey = (a, b) -> compare(a.get("key"), b.get("key"));
    Comparator<Map<String, Object>> byCount =
        Comparator.comparing(bucket -> (Long) bucket.get("doc_count"));
    if (order.isEmpty()) {
      return byCount.reversed().thenComparing(byKey);
    }
    if (order.size() != 1) {
      throw bad("the in-memory engine supports one terms order, not " + order);
    }
    Map.Entry<String, Object> entry = order.entrySet().iterator().next();
    boolean descending = "desc".equals(entry.getValue());
    Comparator<Map<String, Object>> primary;
    if (entry.getKey().equals("_key")) {
      return descending ? byKey.reversed() : byKey;
    } else if (entry.getKey().equals("_count")) {
      primary = byCount;
    } else {
      String[] path = entry.getKey().split(">");
      primary = (a, b) -> compare(metricValue(a, path), metricValue(b, path));
    }
    return (descending ? primary.reversed() : primary).thenComparing(byKey);
  }

  private static Object metricValue(Map<String, Object> bucket, String[] path) {
    Map<String, Object> node = bucket;
    for (String step : path) {
      node = map(node.get(step));
    }
    Object value = node.get("value");
    return value == null ? Double.NEGATIVE_INFINITY : value;
  }

  private static Map<String, Object> nested(String path, Map<String, Object> subs,
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

  private static Map<String, Object> reverseNested(String path, Map<String, Object> subs,
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

  private static Map<String, Object> singleBucket(List<Scope> scopes, Map<String, Object> subs) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("doc_count", (long) scopes.size());
    out.putAll(run(subs, scopes));
    return out;
  }

  // ---- metrics -------------------------------------------------------------------------------

  private static Map<String, Object> metric(String type, String field, List<Scope> scopes) {
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
      out.put("value_as_string", Instant.ofEpochMilli(value.longValue()).toString());
    }
    return out;
  }

  // ---- helpers -------------------------------------------------------------------------------

  /** Returns the values of a field below an object; the field path is absolute. */
  private static List<Object> values(Scope scope, String field) {
    List<Object> out = new ArrayList<>();
    collect(scope.node(), relative(scope, field).split("\\."), 0, out);
    return out;
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
