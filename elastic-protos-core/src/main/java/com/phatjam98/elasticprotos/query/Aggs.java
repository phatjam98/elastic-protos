package com.phatjam98.elasticprotos.query;

import com.google.protobuf.Timestamp;
import com.phatjam98.elasticprotos.proto.Aggregation;
import com.phatjam98.elasticprotos.proto.CalendarInterval;
import com.phatjam98.elasticprotos.proto.CompositeAggregation;
import com.phatjam98.elasticprotos.proto.CompositeDateHistogramSource;
import com.phatjam98.elasticprotos.proto.CompositeHistogramSource;
import com.phatjam98.elasticprotos.proto.CompositeSource;
import com.phatjam98.elasticprotos.proto.CompositeTermsSource;
import com.phatjam98.elasticprotos.proto.DateHistogramAggregation;
import com.phatjam98.elasticprotos.proto.FilterAggregation;
import com.phatjam98.elasticprotos.proto.FiltersAggregation;
import com.phatjam98.elasticprotos.proto.HistogramAggregation;
import com.phatjam98.elasticprotos.proto.KeyedFilter;
import com.phatjam98.elasticprotos.proto.MetricAggregation;
import com.phatjam98.elasticprotos.proto.MetricType;
import com.phatjam98.elasticprotos.proto.PrimitiveValue;
import com.phatjam98.elasticprotos.proto.RangeAggregation;
import com.phatjam98.elasticprotos.proto.RangeBucket;
import com.phatjam98.elasticprotos.proto.SearchCondition;
import com.phatjam98.elasticprotos.proto.TermsAggregation;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Static factories for {@link Aggregation}s, for use with {@link Criteria#aggregate}.
 *
 * <p>Each factory returns the generated builder, so anything not covered here is set on it
 * directly, and sub-aggregations are added with {@code addSubAggregation} or {@link #sub}:
 *
 * <pre>{@code
 * SearchCriteria criteria = Criteria.builder()
 *     .aggregate(
 *         Aggs.sub(Aggs.terms("by_type", "type"), Aggs.sum("total_size", "size_bytes")),
 *         Aggs.dateHistogram("per_month", "filed_at", CalendarInterval.MONTH),
 *         Aggs.range("sizes", "size_bytes",
 *             Aggs.bucket("small", null, 50_000d), Aggs.bucket("large", 50_000d, null)),
 *         Aggs.cardinality("firms", "firm_id"))
 *     .aggregationsOnly()
 *     .build();
 * }</pre>
 */
public final class Aggs {

  private Aggs() {}

  /** Starts a terms aggregation: one bucket per distinct value of the field. */
  public static Aggregation.Builder terms(String name, String field) {
    return named(name, field).setTerms(TermsAggregation.getDefaultInstance());
  }

  /** Starts a date histogram with a calendar interval, such as {@code MONTH}. */
  public static Aggregation.Builder dateHistogram(String name, String field,
      CalendarInterval interval) {
    return named(name, field).setDateHistogram(DateHistogramAggregation.newBuilder()
        .setCalendarInterval(Objects.requireNonNull(interval, "interval")));
  }

  /** Starts a date histogram with a fixed interval, such as {@code "90m"} or {@code "7d"}. */
  public static Aggregation.Builder dateHistogram(String name, String field,
      String fixedInterval) {
    return named(name, field).setDateHistogram(DateHistogramAggregation.newBuilder()
        .setFixedInterval(Objects.requireNonNull(fixedInterval, "fixedInterval")));
  }

  /** Starts a histogram over a numeric field with buckets of the given width. */
  public static Aggregation.Builder histogram(String name, String field, double interval) {
    return named(name, field).setHistogram(HistogramAggregation.newBuilder()
        .setInterval(interval));
  }

  /**
   * Starts a range aggregation; over a date field the bounds are epoch milliseconds.
   *
   * @param name the aggregation name
   * @param field a numeric or date field
   * @param buckets the buckets, from {@link #bucket(String, Double, Double)}
   * @return the builder
   */
  public static Aggregation.Builder range(String name, String field, RangeBucket... buckets) {
    RangeAggregation.Builder range = RangeAggregation.newBuilder();
    for (RangeBucket bucket : buckets) {
      range.addRanges(Objects.requireNonNull(bucket, "bucket"));
    }
    return named(name, field).setRange(range);
  }

  /**
   * Returns a range bucket {@code [from, to)}.
   *
   * @param key the bucket key, or null to let the engine derive one
   * @param from the inclusive lower bound, or null for none
   * @param to the exclusive upper bound, or null for none
   * @return the bucket
   */
  public static RangeBucket bucket(String key, Double from, Double to) {
    RangeBucket.Builder bucket = RangeBucket.newBuilder();
    if (key != null) {
      bucket.setKey(key);
    }
    if (from != null) {
      bucket.setFrom(from);
    }
    if (to != null) {
      bucket.setTo(to);
    }
    return bucket.build();
  }

  /**
   * Returns a date range bucket {@code [from, to)} with Timestamp bounds; for date fields only.
   *
   * @param key the bucket key, or null to let the engine derive one
   * @param from the inclusive lower bound, or null for none
   * @param to the exclusive upper bound, or null for none
   * @return the bucket
   */
  public static RangeBucket bucket(String key, Instant from, Instant to) {
    RangeBucket.Builder bucket = RangeBucket.newBuilder();
    if (key != null) {
      bucket.setKey(key);
    }
    if (from != null) {
      bucket.setFromTime(timestamp(from));
    }
    if (to != null) {
      bucket.setToTime(timestamp(to));
    }
    return bucket.build();
  }

  private static Timestamp timestamp(Instant instant) {
    return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano())
        .build();
  }

  /** Starts a {@code min} metric over a numeric or date field. */
  public static Aggregation.Builder min(String name, String field) {
    return metric(name, field, MetricType.MIN);
  }

  /** Starts a {@code max} metric over a numeric or date field. */
  public static Aggregation.Builder max(String name, String field) {
    return metric(name, field, MetricType.MAX);
  }

  /** Starts an {@code avg} metric over a numeric field. */
  public static Aggregation.Builder avg(String name, String field) {
    return metric(name, field, MetricType.AVG);
  }

  /** Starts a {@code sum} metric over a numeric field. */
  public static Aggregation.Builder sum(String name, String field) {
    return metric(name, field, MetricType.SUM);
  }

  /** Starts a {@code value_count} metric: the number of values of the field. */
  public static Aggregation.Builder count(String name, String field) {
    return metric(name, field, MetricType.VALUE_COUNT);
  }

  /** Starts a {@code cardinality} metric: the approximate number of distinct values. */
  public static Aggregation.Builder cardinality(String name, String field) {
    return metric(name, field, MetricType.CARDINALITY);
  }

  /** Starts a single-value metric of the given type. */
  public static Aggregation.Builder metric(String name, String field, MetricType type) {
    return named(name, field).setMetric(
        MetricAggregation.newBuilder().setType(Objects.requireNonNull(type, "type")));
  }

  /**
   * Starts a filter aggregation: one bucket of the documents that match the condition, in the
   * context of its parent (the root documents at the top level).
   *
   * @param name the aggregation name
   * @param condition the condition, compiled like a condition of the query
   * @return the builder
   */
  public static Aggregation.Builder filter(String name, SearchCondition condition) {
    return Aggregation.newBuilder().setName(Objects.requireNonNull(name, "name"))
        .setFilter(FilterAggregation.newBuilder()
            .setCondition(Objects.requireNonNull(condition, "condition")));
  }

  /**
   * Starts a filter aggregation from a criteria branch, such as {@code
   * Criteria.where("type").equalTo("FILING")}.
   *
   * @param name the aggregation name
   * @param condition the branch whose predicates form the condition
   * @return the builder
   */
  public static Aggregation.Builder filter(String name, Criteria.Branch condition) {
    return filter(name, Objects.requireNonNull(condition, "condition").asCondition());
  }

  /**
   * Starts a filter aggregation over the nested objects at a nested path: one bucket of the
   * objects that match the condition, whose fields must be inside that path.
   *
   * @param name the aggregation name
   * @param nestedPath a field mapped as nested, such as {@code "parties"}
   * @param condition the branch whose predicates form the condition
   * @return the builder
   */
  public static Aggregation.Builder filter(String name, String nestedPath,
      Criteria.Branch condition) {
    return filter(name, condition).setField(Objects.requireNonNull(nestedPath, "nestedPath"));
  }

  /**
   * Starts a filters aggregation: one named bucket per condition. Over nested objects, set the
   * nested path with {@code setField}.
   *
   * @param name the aggregation name
   * @param filters the buckets, from {@link #keyed}
   * @return the builder
   */
  public static Aggregation.Builder filters(String name, KeyedFilter... filters) {
    FiltersAggregation.Builder builder = FiltersAggregation.newBuilder();
    for (KeyedFilter filter : filters) {
      builder.addFilters(Objects.requireNonNull(filter, "filter"));
    }
    return Aggregation.newBuilder().setName(Objects.requireNonNull(name, "name"))
        .setFilters(builder);
  }

  /**
   * Returns one named bucket of a filters aggregation.
   *
   * @param key the bucket key
   * @param condition the condition
   * @return the keyed filter
   */
  public static KeyedFilter keyed(String key, SearchCondition condition) {
    return KeyedFilter.newBuilder().setKey(Objects.requireNonNull(key, "key"))
        .setCondition(Objects.requireNonNull(condition, "condition")).build();
  }

  /**
   * Returns one named bucket of a filters aggregation from a criteria branch.
   *
   * @param key the bucket key
   * @param condition the branch whose predicates form the condition
   * @return the keyed filter
   */
  public static KeyedFilter keyed(String key, Criteria.Branch condition) {
    return keyed(key, Objects.requireNonNull(condition, "condition").asCondition());
  }

  /**
   * Starts a composite aggregation over the given sources; set the page size with {@code
   * getCompositeBuilder().setSize(n)} and page with {@link #after} or {@link CompositePager}.
   *
   * @param name the aggregation name
   * @param sources the sources, in key order
   * @return the builder
   */
  public static Aggregation.Builder composite(String name, CompositeSource.Builder... sources) {
    CompositeAggregation.Builder composite = CompositeAggregation.newBuilder();
    for (CompositeSource.Builder source : sources) {
      composite.addSources(Objects.requireNonNull(source, "source"));
    }
    return Aggregation.newBuilder().setName(Objects.requireNonNull(name, "name"))
        .setComposite(composite);
  }

  /**
   * Starts a terms source of a composite aggregation; set {@code setOrder} or {@code
   * setMissingBucket} on it as needed.
   *
   * @param name the source name in the bucket keys
   * @param field the field
   * @return the builder
   */
  public static CompositeSource.Builder termsSource(String name, String field) {
    return source(name, field).setTerms(CompositeTermsSource.getDefaultInstance());
  }

  /** Starts a date histogram source of a composite aggregation with a calendar interval. */
  public static CompositeSource.Builder dateHistogramSource(String name, String field,
      CalendarInterval interval) {
    return source(name, field).setDateHistogram(CompositeDateHistogramSource.newBuilder()
        .setCalendarInterval(Objects.requireNonNull(interval, "interval")));
  }

  /** Starts a date histogram source of a composite aggregation with a fixed interval. */
  public static CompositeSource.Builder dateHistogramSource(String name, String field,
      String fixedInterval) {
    return source(name, field).setDateHistogram(CompositeDateHistogramSource.newBuilder()
        .setFixedInterval(Objects.requireNonNull(fixedInterval, "fixedInterval")));
  }

  /** Starts a histogram source of a composite aggregation. */
  public static CompositeSource.Builder histogramSource(String name, String field,
      double interval) {
    return source(name, field).setHistogram(CompositeHistogramSource.newBuilder()
        .setInterval(interval));
  }

  private static CompositeSource.Builder source(String name, String field) {
    return CompositeSource.newBuilder().setName(Objects.requireNonNull(name, "name"))
        .setField(Objects.requireNonNull(field, "field"));
  }

  /**
   * Sets the after key of a composite aggregation, to request the page after the one that
   * returned it ({@link AggregationResults#afterKey}).
   *
   * @param composite a composite aggregation builder
   * @param afterKey source name to value: strings, numbers, booleans, or null for a missing
   *     bucket; empty starts again at the first page
   * @return {@code composite}
   * @throws IllegalArgumentException when a value is of another type
   */
  public static Aggregation.Builder after(Aggregation.Builder composite,
      Map<String, ?> afterKey) {
    CompositeAggregation.Builder builder = composite.getCompositeBuilder().clearAfter();
    afterKey.forEach((source, value) -> builder.putAfter(source,
        value == null ? PrimitiveValue.getDefaultInstance() : Values.primitive(value)));
    return composite;
  }

  /**
   * Adds sub-aggregations to a bucket aggregation.
   *
   * @param parent the bucket aggregation
   * @param subs its sub-aggregations
   * @return {@code parent}
   */
  public static Aggregation.Builder sub(Aggregation.Builder parent, Aggregation... subs) {
    for (Aggregation sub : subs) {
      parent.addSubAggregation(Objects.requireNonNull(sub, "sub"));
    }
    return parent;
  }

  /**
   * Adds sub-aggregations, given as builders, to a bucket aggregation.
   *
   * @param parent the bucket aggregation
   * @param subs its sub-aggregations; each is built when added
   * @return {@code parent}
   */
  public static Aggregation.Builder sub(Aggregation.Builder parent, Aggregation.Builder... subs) {
    for (Aggregation.Builder sub : subs) {
      parent.addSubAggregation(Objects.requireNonNull(sub, "sub").build());
    }
    return parent;
  }

  private static Aggregation.Builder named(String name, String field) {
    return Aggregation.newBuilder()
        .setName(Objects.requireNonNull(name, "name"))
        .setField(Objects.requireNonNull(field, "field"));
  }
}
