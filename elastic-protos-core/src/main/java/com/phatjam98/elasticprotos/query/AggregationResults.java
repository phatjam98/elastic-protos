package com.phatjam98.elasticprotos.query;

import com.phatjam98.elasticprotos.engine.SearchResult;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * Typed access to the aggregation results of a search compiled from {@code
 * SearchCriteria.aggregation}.
 *
 * <p>Results are looked up by the names given in the criteria. The {@code nested} and {@code
 * reverse_nested} wrappers the compiler adds around aggregations on nested fields ({@code
 * <name>__nested}, {@code <name>__nested2}, ..., {@code <name>__reverse}) are unwrapped by their
 * names, which user names cannot clash with because they must not contain {@code __}:
 *
 * <pre>{@code
 * AggregationResults aggs = AggregationResults.of(engine.search(indices, compiled.body()));
 * for (AggregationResults.Bucket bucket : aggs.buckets("by_type")) {
 *   long count = bucket.docCount();
 *   Double total = bucket.sub().metric("total_size").value();
 * }
 * }</pre>
 *
 * <p>Both list-shaped and keyed (object-shaped) buckets are read. Instances are immutable; the
 * copy of the raw map is shallow.
 */
public final class AggregationResults {

  private static final AggregationResults EMPTY = new AggregationResults(Map.of());

  private final Map<String, Object> raw;

  private AggregationResults(Map<String, Object> raw) {
    this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(raw));
  }

  /** Returns results without any aggregation. */
  public static AggregationResults empty() {
    return EMPTY;
  }

  /**
   * Reads the aggregations of a search result.
   *
   * @param result a search result
   * @return its aggregation results; empty when it has none
   */
  public static AggregationResults of(SearchResult result) {
    return of(Objects.requireNonNull(result, "result").aggregations());
  }

  /**
   * Reads a raw {@code aggregations} object, as the engines return it.
   *
   * @param aggregations the raw aggregations; null means none
   * @return the results
   */
  public static AggregationResults of(Map<String, Object> aggregations) {
    return aggregations == null || aggregations.isEmpty()
        ? EMPTY
        : new AggregationResults(aggregations);
  }

  /** Returns the raw aggregations, including any wrappers. */
  public Map<String, Object> raw() {
    return raw;
  }

  /** Returns whether there are no aggregation results. */
  public boolean isEmpty() {
    return names().isEmpty();
  }

  /** Returns the names of the aggregations present, with wrappers unwrapped, in response order. */
  public Set<String> names() {
    Set<String> names = new LinkedHashSet<>();
    for (String key : raw.keySet()) {
      if (AggregationCompiler.RESERVED_NAMES.contains(key)) {
        continue;
      }
      int wrapper = key.indexOf(AggregationCompiler.WRAPPER);
      names.add(wrapper < 0 ? key : key.substring(0, wrapper));
    }
    return Collections.unmodifiableSet(names);
  }

  /** Returns whether an aggregation of this name is present. */
  public boolean has(String name) {
    return find(raw, name) != null;
  }

  /**
   * Returns the buckets of a bucket aggregation (terms, histogram, date histogram, range).
   *
   * @param name the aggregation name
   * @return the buckets in response order
   * @throws NoSuchElementException when there is no aggregation of this name
   * @throws IllegalStateException when it is a metric
   */
  public List<Bucket> buckets(String name) {
    Map<String, Object> aggregation = require(name);
    Object buckets = aggregation.get("buckets");
    List<Bucket> out = new ArrayList<>();
    if (buckets instanceof List<?> list) {
      for (Object bucket : list) {
        out.add(bucket(null, map(bucket)));
      }
    } else if (buckets instanceof Map<?, ?> keyed) {
      for (Map.Entry<?, ?> entry : keyed.entrySet()) {
        out.add(bucket(String.valueOf(entry.getKey()), map(entry.getValue())));
      }
    } else {
      throw new IllegalStateException("Aggregation '" + name + "' has no buckets; it is a"
          + " metric, read it with metric(\"" + name + "\")");
    }
    return Collections.unmodifiableList(out);
  }

  /**
   * Returns the number of documents of a terms aggregation that fell outside its buckets.
   *
   * @param name the aggregation name
   * @return {@code sum_other_doc_count}, or 0 when the response has none
   * @throws NoSuchElementException when there is no aggregation of this name
   */
  public long sumOtherDocCount(String name) {
    Object value = require(name).get("sum_other_doc_count");
    return value instanceof Number number ? number.longValue() : 0L;
  }

  /**
   * Returns the value of a metric aggregation.
   *
   * @param name the aggregation name
   * @return the value; absent ({@code value} null) when no document had the field
   * @throws NoSuchElementException when there is no aggregation of this name
   * @throws IllegalStateException when it is a bucket aggregation
   */
  public MetricValue metric(String name) {
    Map<String, Object> aggregation = require(name);
    if (!aggregation.containsKey("value")) {
      throw new IllegalStateException("Aggregation '" + name + "' has no value; it is a bucket"
          + " aggregation, read it with buckets(\"" + name + "\")");
    }
    Object valueAsString = aggregation.get("value_as_string");
    return new MetricValue(toDouble(aggregation.get("value")),
        valueAsString == null ? null : String.valueOf(valueAsString));
  }

  private Map<String, Object> require(String name) {
    Map<String, Object> aggregation = find(raw, name);
    if (aggregation == null) {
      throw new NoSuchElementException(
          "No aggregation named '" + name + "'; present: " + names());
    }
    return aggregation;
  }

  /** Finds an aggregation by name, descending into its {@code name__*} wrappers. */
  private static Map<String, Object> find(Map<String, Object> level, String name) {
    Object direct = level.get(name);
    if (direct instanceof Map) {
      return map(direct);
    }
    String prefix = name + AggregationCompiler.WRAPPER;
    for (Map.Entry<String, Object> entry : level.entrySet()) {
      if (entry.getKey().startsWith(prefix) && entry.getValue() instanceof Map) {
        Map<String, Object> found = find(map(entry.getValue()), name);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private static Bucket bucket(String mapKey, Map<String, Object> bucket) {
    Map<String, Object> sub = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : bucket.entrySet()) {
      if (!AggregationCompiler.RESERVED_NAMES.contains(entry.getKey())) {
        sub.put(entry.getKey(), entry.getValue());
      }
    }
    Object key = bucket.containsKey("key") ? key(bucket.get("key")) : mapKey;
    Object docCount = bucket.get("doc_count");
    return new Bucket(key, string(bucket.get("key_as_string")),
        docCount instanceof Number number ? number.longValue() : 0L,
        toDouble(bucket.get("from")), toDouble(bucket.get("to")),
        string(bucket.get("from_as_string")), string(bucket.get("to_as_string")),
        of(sub));
  }

  /** Normalizes a bucket key: integral JSON numbers become Long, others Double. */
  private static Object key(Object key) {
    if (key instanceof Integer || key instanceof Long || key instanceof Short
        || key instanceof Byte) {
      return ((Number) key).longValue();
    }
    if (key instanceof BigInteger big) {
      return big.longValue();
    }
    if (key instanceof Double || key instanceof Float || key instanceof BigDecimal) {
      return ((Number) key).doubleValue();
    }
    return key;
  }

  private static Double toDouble(Object value) {
    return value instanceof Number number ? number.doubleValue() : null;
  }

  private static String string(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : Map.of();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof AggregationResults that && raw.equals(that.raw);
  }

  @Override
  public int hashCode() {
    return raw.hashCode();
  }

  @Override
  public String toString() {
    return "AggregationResults" + raw;
  }

  /**
   * One bucket of a bucket aggregation.
   *
   * @param key the bucket key: a String for keyword terms and range buckets, a Long for numeric
   *     terms on integer fields, date terms and date histograms (epoch milliseconds), a Double for
   *     histograms and terms on floating-point fields
   * @param keyAsString the formatted key ({@code key_as_string}) for dates, booleans and
   *     formatted histograms; null otherwise
   * @param docCount the number of documents in the bucket
   * @param from the inclusive lower bound of a range bucket, or null
   * @param to the exclusive upper bound of a range bucket, or null
   * @param fromAsString the formatted lower bound of a date range bucket, or null
   * @param toAsString the formatted upper bound of a date range bucket, or null
   * @param sub the results of the sub-aggregations in this bucket
   */
  public record Bucket(Object key, String keyAsString, long docCount, Double from, Double to,
      String fromAsString, String toAsString, AggregationResults sub) {

    /** Returns {@code keyAsString} when present, otherwise the key as a string. */
    public String keyString() {
      return keyAsString != null ? keyAsString : String.valueOf(key);
    }
  }

  /**
   * The value of a single-value metric.
   *
   * @param value the value, or null when no document had the field (min, max and avg of nothing)
   * @param valueAsString the formatted value, such as a date for min and max of a date field, or
   *     null
   */
  public record MetricValue(Double value, String valueAsString) {

    /** Returns whether there is a value. */
    public boolean isPresent() {
      return value != null;
    }

    /**
     * Returns the value rounded to a long, for counts and date metrics (epoch milliseconds).
     *
     * @throws IllegalStateException when there is no value
     */
    public long asLong() {
      if (value == null) {
        throw new IllegalStateException("The metric has no value");
      }
      return Math.round(value);
    }
  }
}
