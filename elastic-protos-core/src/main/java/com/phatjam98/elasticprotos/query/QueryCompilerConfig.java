package com.phatjam98.elasticprotos.query;

import java.time.Duration;
import java.util.Objects;

/**
 * Settings of a {@link QueryCompiler}.
 *
 * @param allowLeadingWildcards whether {@code TEXT_CONTAINS} on keyword fields and {@code
 *     TEXT_SUFFIX} may emit wildcard queries that start with {@code *}; these scan every term of
 *     the field and are disabled by default
 * @param lenient false to throw on fields the index does not map; true to skip them and record a
 *     warning
 * @param trackTotalHits value of {@code track_total_hits}: a Boolean, an Integer, or null to leave
 *     it out of the body
 * @param maxResultWindow the largest {@code from + size} allowed without a point in time; match it
 *     to the index setting {@code index.max_result_window}
 * @param defaultSize page size when the criteria do not set one
 * @param defaultKeepAlive point-in-time keep-alive when the pagination does not set one
 * @param groupNested whether predicates of one condition on the same nested path are combined so
 *     that they must match the same nested element
 * @param shardDocTiebreaker whether point-in-time searches get a {@code _shard_doc} tiebreaker
 *     sort; turn it off for engines without {@code _shard_doc} (OpenSearch before 3.3), whose
 *     pages are then only complete when the sort ends with a unique field
 */
public record QueryCompilerConfig(
    boolean allowLeadingWildcards,
    boolean lenient,
    Object trackTotalHits,
    int maxResultWindow,
    int defaultSize,
    Duration defaultKeepAlive,
    boolean groupNested,
    boolean shardDocTiebreaker) {

  /** Validates the settings. */
  public QueryCompilerConfig {
    if (trackTotalHits != null
        && !(trackTotalHits instanceof Boolean)
        && !(trackTotalHits instanceof Integer)) {
      throw new IllegalArgumentException(
          "trackTotalHits must be a Boolean, an Integer or null, got " + trackTotalHits);
    }
    if (maxResultWindow <= 0) {
      throw new IllegalArgumentException("maxResultWindow must be positive: " + maxResultWindow);
    }
    if (defaultSize < 0) {
      throw new IllegalArgumentException("defaultSize must not be negative: " + defaultSize);
    }
    Objects.requireNonNull(defaultKeepAlive, "defaultKeepAlive");
    if (defaultKeepAlive.isNegative() || defaultKeepAlive.isZero()) {
      throw new IllegalArgumentException("defaultKeepAlive must be positive: " + defaultKeepAlive);
    }
  }

  /** Creates settings with the {@code _shard_doc} tiebreaker enabled. */
  public QueryCompilerConfig(boolean allowLeadingWildcards, boolean lenient, Object trackTotalHits,
      int maxResultWindow, int defaultSize, Duration defaultKeepAlive, boolean groupNested) {
    this(allowLeadingWildcards, lenient, trackTotalHits, maxResultWindow, defaultSize,
        defaultKeepAlive, groupNested, true);
  }

  /**
   * Returns the defaults: strict fields, no leading wildcards, exact totals, a result window of
   * 10000, pages of 10, a one minute keep-alive, nested grouping and the {@code _shard_doc}
   * tiebreaker.
   */
  public static QueryCompilerConfig standard() {
    return new QueryCompilerConfig(false, false, Boolean.TRUE, 10_000, 10, Duration.ofMinutes(1),
        true);
  }

  /** Returns a copy with {@code allowLeadingWildcards} replaced. */
  public QueryCompilerConfig withAllowLeadingWildcards(boolean value) {
    return new QueryCompilerConfig(value, lenient, trackTotalHits, maxResultWindow, defaultSize,
        defaultKeepAlive, groupNested, shardDocTiebreaker);
  }

  /** Returns a copy with {@code lenient} replaced. */
  public QueryCompilerConfig withLenient(boolean value) {
    return new QueryCompilerConfig(allowLeadingWildcards, value, trackTotalHits, maxResultWindow,
        defaultSize, defaultKeepAlive, groupNested, shardDocTiebreaker);
  }

  /** Returns a copy with {@code trackTotalHits} replaced. */
  public QueryCompilerConfig withTrackTotalHits(Object value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, value, maxResultWindow,
        defaultSize, defaultKeepAlive, groupNested, shardDocTiebreaker);
  }

  /** Returns a copy with {@code maxResultWindow} replaced. */
  public QueryCompilerConfig withMaxResultWindow(int value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, trackTotalHits, value,
        defaultSize, defaultKeepAlive, groupNested, shardDocTiebreaker);
  }

  /** Returns a copy with {@code defaultSize} replaced. */
  public QueryCompilerConfig withDefaultSize(int value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, trackTotalHits,
        maxResultWindow, value, defaultKeepAlive, groupNested, shardDocTiebreaker);
  }

  /** Returns a copy with {@code defaultKeepAlive} replaced. */
  public QueryCompilerConfig withDefaultKeepAlive(Duration value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, trackTotalHits,
        maxResultWindow, defaultSize, value, groupNested, shardDocTiebreaker);
  }

  /** Returns a copy with {@code groupNested} replaced. */
  public QueryCompilerConfig withGroupNested(boolean value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, trackTotalHits,
        maxResultWindow, defaultSize, defaultKeepAlive, value, shardDocTiebreaker);
  }

  /** Returns a copy with {@code shardDocTiebreaker} replaced. */
  public QueryCompilerConfig withShardDocTiebreaker(boolean value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, trackTotalHits,
        maxResultWindow, defaultSize, defaultKeepAlive, groupNested, value);
  }
}
