package com.phatjam98.elasticprotos.query;

import java.time.Duration;
import java.util.Objects;

/**
 * Settings of a {@link QueryCompiler}.
 *
 * <p>Criteria often come straight from API clients, so everything that lets a client run code on
 * the cluster or scan every term of a field is off by default: script sorts, regular expressions
 * and leading wildcards.
 *
 * @param allowLeadingWildcards whether {@code TEXT_CONTAINS} on keyword fields and {@code
 *     TEXT_SUFFIX} may emit wildcard queries that start with {@code *}, and whether a {@code
 *     TEXT_REGEX} pattern may start with something other than a literal character; these scan
 *     every term of the field and are disabled by default
 * @param lenient false to throw on fields the index does not map; true to skip them and record a
 *     warning
 * @param trackTotalHits value of {@code track_total_hits}: a Boolean, an Integer, or null to leave
 *     it out of the body
 * @param maxResultWindow the largest {@code from + size} allowed without a point in time; match it
 *     to the index setting {@code index.max_result_window}
 * @param defaultSize page size when the criteria do not set one; at most {@code maxPageSize}
 * @param defaultKeepAlive point-in-time keep-alive when the pagination does not set one
 * @param groupNested whether predicates of one condition on the same nested path are combined so
 *     that they must match the same nested element
 * @param shardDocTiebreaker whether point-in-time searches get a {@code _shard_doc} tiebreaker
 *     sort; turn it off for engines without {@code _shard_doc} (OpenSearch before 3.3), whose
 *     pages are then only complete when the sort ends with a unique field
 * @param allowScriptSort whether a {@code ScriptSort} is accepted; it runs a client-supplied
 *     Painless script, or a stored script, on the cluster and is disabled by default
 * @param allowRegex whether {@code TEXT_REGEX} is accepted; a regular expression can be
 *     arbitrarily expensive, so it is disabled by default
 * @param maxPageSize the largest page {@code size}, with or without a point in time; default
 *     {@value #DEFAULT_MAX_PAGE_SIZE}, the default {@code index.max_result_window}
 */
public record QueryCompilerConfig(
    boolean allowLeadingWildcards,
    boolean lenient,
    Object trackTotalHits,
    int maxResultWindow,
    int defaultSize,
    Duration defaultKeepAlive,
    boolean groupNested,
    boolean shardDocTiebreaker,
    boolean allowScriptSort,
    boolean allowRegex,
    int maxPageSize) {

  /** The default {@link #maxPageSize()}. */
  public static final int DEFAULT_MAX_PAGE_SIZE = 10_000;

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
    if (maxPageSize <= 0) {
      throw new IllegalArgumentException("maxPageSize must be positive: " + maxPageSize);
    }
    if (defaultSize > maxPageSize) {
      throw new IllegalArgumentException("defaultSize " + defaultSize
          + " must not exceed maxPageSize " + maxPageSize);
    }
    Objects.requireNonNull(defaultKeepAlive, "defaultKeepAlive");
    if (defaultKeepAlive.isNegative() || defaultKeepAlive.isZero()) {
      throw new IllegalArgumentException("defaultKeepAlive must be positive: " + defaultKeepAlive);
    }
  }

  /**
   * Creates settings without script sorts or regular expressions and with pages of at most
   * {@value #DEFAULT_MAX_PAGE_SIZE}.
   */
  public QueryCompilerConfig(boolean allowLeadingWildcards, boolean lenient, Object trackTotalHits,
      int maxResultWindow, int defaultSize, Duration defaultKeepAlive, boolean groupNested,
      boolean shardDocTiebreaker) {
    this(allowLeadingWildcards, lenient, trackTotalHits, maxResultWindow, defaultSize,
        defaultKeepAlive, groupNested, shardDocTiebreaker, false, false, DEFAULT_MAX_PAGE_SIZE);
  }

  /** Creates settings like the eight-argument constructor, with the tiebreaker enabled. */
  public QueryCompilerConfig(boolean allowLeadingWildcards, boolean lenient, Object trackTotalHits,
      int maxResultWindow, int defaultSize, Duration defaultKeepAlive, boolean groupNested) {
    this(allowLeadingWildcards, lenient, trackTotalHits, maxResultWindow, defaultSize,
        defaultKeepAlive, groupNested, true);
  }

  /**
   * Returns the defaults: strict fields, no leading wildcards, no script sorts, no regular
   * expressions, exact totals, a result window of 10000, pages of 10 and at most 10000, a one
   * minute keep-alive, nested grouping and the {@code _shard_doc} tiebreaker.
   */
  public static QueryCompilerConfig standard() {
    return new QueryCompilerConfig(false, false, Boolean.TRUE, 10_000, 10, Duration.ofMinutes(1),
        true);
  }

  /** Returns a copy with {@code allowLeadingWildcards} replaced. */
  public QueryCompilerConfig withAllowLeadingWildcards(boolean value) {
    return new QueryCompilerConfig(value, lenient, trackTotalHits, maxResultWindow, defaultSize,
        defaultKeepAlive, groupNested, shardDocTiebreaker, allowScriptSort, allowRegex,
        maxPageSize);
  }

  /** Returns a copy with {@code lenient} replaced. */
  public QueryCompilerConfig withLenient(boolean value) {
    return new QueryCompilerConfig(allowLeadingWildcards, value, trackTotalHits, maxResultWindow,
        defaultSize, defaultKeepAlive, groupNested, shardDocTiebreaker, allowScriptSort, allowRegex,
        maxPageSize);
  }

  /** Returns a copy with {@code trackTotalHits} replaced. */
  public QueryCompilerConfig withTrackTotalHits(Object value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, value, maxResultWindow,
        defaultSize, defaultKeepAlive, groupNested, shardDocTiebreaker, allowScriptSort, allowRegex,
        maxPageSize);
  }

  /** Returns a copy with {@code maxResultWindow} replaced. */
  public QueryCompilerConfig withMaxResultWindow(int value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, trackTotalHits, value,
        defaultSize, defaultKeepAlive, groupNested, shardDocTiebreaker, allowScriptSort, allowRegex,
        maxPageSize);
  }

  /** Returns a copy with {@code defaultSize} replaced. */
  public QueryCompilerConfig withDefaultSize(int value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, trackTotalHits, maxResultWindow,
        value, defaultKeepAlive, groupNested, shardDocTiebreaker, allowScriptSort, allowRegex,
        maxPageSize);
  }

  /** Returns a copy with {@code defaultKeepAlive} replaced. */
  public QueryCompilerConfig withDefaultKeepAlive(Duration value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, trackTotalHits, maxResultWindow,
        defaultSize, value, groupNested, shardDocTiebreaker, allowScriptSort, allowRegex,
        maxPageSize);
  }

  /** Returns a copy with {@code groupNested} replaced. */
  public QueryCompilerConfig withGroupNested(boolean value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, trackTotalHits, maxResultWindow,
        defaultSize, defaultKeepAlive, value, shardDocTiebreaker, allowScriptSort, allowRegex,
        maxPageSize);
  }

  /** Returns a copy with {@code shardDocTiebreaker} replaced. */
  public QueryCompilerConfig withShardDocTiebreaker(boolean value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, trackTotalHits, maxResultWindow,
        defaultSize, defaultKeepAlive, groupNested, value, allowScriptSort, allowRegex,
        maxPageSize);
  }

  /** Returns a copy with {@code allowScriptSort} replaced. */
  public QueryCompilerConfig withAllowScriptSort(boolean value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, trackTotalHits, maxResultWindow,
        defaultSize, defaultKeepAlive, groupNested, shardDocTiebreaker, value, allowRegex,
        maxPageSize);
  }

  /** Returns a copy with {@code allowRegex} replaced. */
  public QueryCompilerConfig withAllowRegex(boolean value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, trackTotalHits, maxResultWindow,
        defaultSize, defaultKeepAlive, groupNested, shardDocTiebreaker, allowScriptSort, value,
        maxPageSize);
  }

  /** Returns a copy with {@code maxPageSize} replaced. */
  public QueryCompilerConfig withMaxPageSize(int value) {
    return new QueryCompilerConfig(allowLeadingWildcards, lenient, trackTotalHits, maxResultWindow,
        defaultSize, defaultKeepAlive, groupNested, shardDocTiebreaker, allowScriptSort, allowRegex,
        value);
  }
}
