package com.phatjam98.elasticprotos.query;

import com.google.protobuf.Timestamp;
import com.phatjam98.elasticprotos.proto.Aggregation;
import com.phatjam98.elasticprotos.proto.CalendarInterval;
import com.phatjam98.elasticprotos.proto.DateHistogramAggregation;
import com.phatjam98.elasticprotos.proto.HistogramAggregation;
import com.phatjam98.elasticprotos.proto.MetricAggregation;
import com.phatjam98.elasticprotos.proto.MetricType;
import com.phatjam98.elasticprotos.proto.RangeAggregation;
import com.phatjam98.elasticprotos.proto.RangeBucket;
import com.phatjam98.elasticprotos.proto.TermsAggregation;
import java.time.Instant;
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
