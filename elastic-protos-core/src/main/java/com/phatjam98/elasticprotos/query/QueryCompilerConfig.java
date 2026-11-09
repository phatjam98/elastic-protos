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
 * <p>Start from {@link #standard()} and change single settings with the {@code with*} methods, or
 * set several at once with {@link #builder()} or {@link #toBuilder()}:
 *
 * <pre>{@code
 * QueryCompilerConfig config = QueryCompilerConfig.builder()
 *     .lenient(true)
 *     .trackTotalHits(TrackTotalHits.upTo(1000))
 *     .maxPageSize(1_000)
 *     .build();
 * }</pre>
 *
 * <p>Settings:
 *
 * <ul>
 *   <li>{@code allowLeadingWildcards}: whether {@code TEXT_CONTAINS} on keyword fields and {@code
 *       TEXT_SUFFIX} may emit wildcard queries that start with {@code *}, and whether a {@code
 *       TEXT_REGEX} pattern may start with something other than a literal character; these scan
 *       every term of the field and are disabled by default
 *   <li>{@code lenient}: false to throw on fields the index does not map; true to skip them and
 *       record a warning
 *   <li>{@code trackTotalHits}: value of {@code track_total_hits}; default {@link
 *       TrackTotalHits#exact()}; {@link TrackTotalHits#engineDefault()} leaves it out of the body
 *   <li>{@code maxResultWindow}: the largest {@code from + size} allowed without a point in time;
 *       match it to the index setting {@code index.max_result_window}
 *   <li>{@code defaultSize}: page size when the criteria do not set one; at most {@code
 *       maxPageSize}
 *   <li>{@code defaultKeepAlive}: point-in-time keep-alive when the pagination does not set one
 *   <li>{@code groupNested}: whether predicates of one condition on the same nested path are
 *       combined so that they must match the same nested element
 *   <li>{@code shardDocTiebreaker}: whether point-in-time searches get a {@code _shard_doc}
 *       tiebreaker sort; turn it off for engines without {@code _shard_doc} (OpenSearch before
 *       3.3), whose pages are then only complete when the sort ends with a unique field
 *   <li>{@code allowScriptSort}: whether a {@code ScriptSort} is accepted; it runs a
 *       client-supplied Painless script, or a stored script, on the cluster and is disabled by
 *       default
 *   <li>{@code allowRegex}: whether {@code TEXT_REGEX} is accepted; a regular expression can be
 *       arbitrarily expensive, so it is disabled by default
 *   <li>{@code maxPageSize}: the largest page {@code size}, with or without a point in time;
 *       default {@value #DEFAULT_MAX_PAGE_SIZE}, the default {@code index.max_result_window}
 *   <li>{@code maxTermsSize}: the largest {@code size} of a terms aggregation; default {@value
 *       #DEFAULT_MAX_TERMS_SIZE}
 * </ul>
 */
public final class QueryCompilerConfig {

  /** The default {@link #maxPageSize()}. */
  public static final int DEFAULT_MAX_PAGE_SIZE = 10_000;

  /** The default {@link #maxTermsSize()}. */
  public static final int DEFAULT_MAX_TERMS_SIZE = 1_000;

  private final boolean allowLeadingWildcards;
  private final boolean lenient;
  private final TrackTotalHits trackTotalHits;
  private final int maxResultWindow;
  private final int defaultSize;
  private final Duration defaultKeepAlive;
  private final boolean groupNested;
  private final boolean shardDocTiebreaker;
  private final boolean allowScriptSort;
  private final boolean allowRegex;
  private final int maxPageSize;
  private final int maxTermsSize;

  private QueryCompilerConfig(Builder builder) {
    if (builder.maxResultWindow <= 0) {
      throw new IllegalArgumentException(
          "maxResultWindow must be positive: " + builder.maxResultWindow);
    }
    if (builder.defaultSize < 0) {
      throw new IllegalArgumentException(
          "defaultSize must not be negative: " + builder.defaultSize);
    }
    if (builder.maxPageSize <= 0) {
      throw new IllegalArgumentException("maxPageSize must be positive: " + builder.maxPageSize);
    }
    if (builder.maxTermsSize <= 0) {
      throw new IllegalArgumentException(
          "maxTermsSize must be positive: " + builder.maxTermsSize);
    }
    if (builder.defaultSize > builder.maxPageSize) {
      throw new IllegalArgumentException("defaultSize " + builder.defaultSize
          + " must not exceed maxPageSize " + builder.maxPageSize);
    }
    Objects.requireNonNull(builder.trackTotalHits, "trackTotalHits");
    Objects.requireNonNull(builder.defaultKeepAlive, "defaultKeepAlive");
    if (builder.defaultKeepAlive.isNegative() || builder.defaultKeepAlive.isZero()) {
      throw new IllegalArgumentException(
          "defaultKeepAlive must be positive: " + builder.defaultKeepAlive);
    }
    this.allowLeadingWildcards = builder.allowLeadingWildcards;
    this.lenient = builder.lenient;
    this.trackTotalHits = builder.trackTotalHits;
    this.maxResultWindow = builder.maxResultWindow;
    this.defaultSize = builder.defaultSize;
    this.defaultKeepAlive = builder.defaultKeepAlive;
    this.groupNested = builder.groupNested;
    this.shardDocTiebreaker = builder.shardDocTiebreaker;
    this.allowScriptSort = builder.allowScriptSort;
    this.allowRegex = builder.allowRegex;
    this.maxPageSize = builder.maxPageSize;
    this.maxTermsSize = builder.maxTermsSize;
  }

  /**
   * Returns the defaults: strict fields, no leading wildcards, no script sorts, no regular
   * expressions, exact totals, a result window of 10000, pages of 10 and at most 10000, a one
   * minute keep-alive, nested grouping, the {@code _shard_doc} tiebreaker and terms aggregations
   * of at most 1000 buckets.
   */
  public static QueryCompilerConfig standard() {
    return builder().build();
  }

  /** Returns a builder holding the {@link #standard()} settings. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns a builder holding these settings. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /** Returns whether wildcard queries may start with {@code *}. */
  public boolean allowLeadingWildcards() {
    return allowLeadingWildcards;
  }

  /** Returns whether unmapped fields are skipped with a warning instead of failing. */
  public boolean lenient() {
    return lenient;
  }

  /** Returns the {@code track_total_hits} setting; never null. */
  public TrackTotalHits trackTotalHits() {
    return trackTotalHits;
  }

  /** Returns the largest {@code from + size} without a point in time. */
  public int maxResultWindow() {
    return maxResultWindow;
  }

  /** Returns the page size used when the criteria set none. */
  public int defaultSize() {
    return defaultSize;
  }

  /** Returns the point-in-time keep-alive used when the pagination sets none. */
  public Duration defaultKeepAlive() {
    return defaultKeepAlive;
  }

  /** Returns whether predicates on the same nested path must match the same element. */
  public boolean groupNested() {
    return groupNested;
  }

  /** Returns whether point-in-time searches get a {@code _shard_doc} tiebreaker. */
  public boolean shardDocTiebreaker() {
    return shardDocTiebreaker;
  }

  /** Returns whether script sorts are accepted. */
  public boolean allowScriptSort() {
    return allowScriptSort;
  }

  /** Returns whether {@code TEXT_REGEX} is accepted. */
  public boolean allowRegex() {
    return allowRegex;
  }

  /** Returns the largest page size. */
  public int maxPageSize() {
    return maxPageSize;
  }

  /** Returns the largest {@code size} of a terms aggregation. */
  public int maxTermsSize() {
    return maxTermsSize;
  }

  /** Returns a copy with {@code allowLeadingWildcards} replaced. */
  public QueryCompilerConfig withAllowLeadingWildcards(boolean value) {
    return toBuilder().allowLeadingWildcards(value).build();
  }

  /** Returns a copy with {@code lenient} replaced. */
  public QueryCompilerConfig withLenient(boolean value) {
    return toBuilder().lenient(value).build();
  }

  /**
   * Returns a copy with {@code trackTotalHits} replaced; {@link TrackTotalHits#engineDefault()}
   * leaves it out of the body.
   *
   * @throws NullPointerException when {@code value} is null
   */
  public QueryCompilerConfig withTrackTotalHits(TrackTotalHits value) {
    return toBuilder().trackTotalHits(value).build();
  }

  /** Returns a copy with {@code maxResultWindow} replaced. */
  public QueryCompilerConfig withMaxResultWindow(int value) {
    return toBuilder().maxResultWindow(value).build();
  }

  /** Returns a copy with {@code defaultSize} replaced. */
  public QueryCompilerConfig withDefaultSize(int value) {
    return toBuilder().defaultSize(value).build();
  }

  /** Returns a copy with {@code defaultKeepAlive} replaced. */
  public QueryCompilerConfig withDefaultKeepAlive(Duration value) {
    return toBuilder().defaultKeepAlive(value).build();
  }

  /** Returns a copy with {@code groupNested} replaced. */
  public QueryCompilerConfig withGroupNested(boolean value) {
    return toBuilder().groupNested(value).build();
  }

  /** Returns a copy with {@code shardDocTiebreaker} replaced. */
  public QueryCompilerConfig withShardDocTiebreaker(boolean value) {
    return toBuilder().shardDocTiebreaker(value).build();
  }

  /** Returns a copy with {@code allowScriptSort} replaced. */
  public QueryCompilerConfig withAllowScriptSort(boolean value) {
    return toBuilder().allowScriptSort(value).build();
  }

  /** Returns a copy with {@code allowRegex} replaced. */
  public QueryCompilerConfig withAllowRegex(boolean value) {
    return toBuilder().allowRegex(value).build();
  }

  /** Returns a copy with {@code maxPageSize} replaced. */
  public QueryCompilerConfig withMaxPageSize(int value) {
    return toBuilder().maxPageSize(value).build();
  }

  /** Returns a copy with {@code maxTermsSize} replaced. */
  public QueryCompilerConfig withMaxTermsSize(int value) {
    return toBuilder().maxTermsSize(value).build();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof QueryCompilerConfig that
        && allowLeadingWildcards == that.allowLeadingWildcards
        && lenient == that.lenient
        && trackTotalHits.equals(that.trackTotalHits)
        && maxResultWindow == that.maxResultWindow
        && defaultSize == that.defaultSize
        && defaultKeepAlive.equals(that.defaultKeepAlive)
        && groupNested == that.groupNested
        && shardDocTiebreaker == that.shardDocTiebreaker
        && allowScriptSort == that.allowScriptSort
        && allowRegex == that.allowRegex
        && maxPageSize == that.maxPageSize
        && maxTermsSize == that.maxTermsSize;
  }

  @Override
  public int hashCode() {
    return Objects.hash(allowLeadingWildcards, lenient, trackTotalHits, maxResultWindow,
        defaultSize, defaultKeepAlive, groupNested, shardDocTiebreaker, allowScriptSort,
        allowRegex, maxPageSize, maxTermsSize);
  }

  @Override
  public String toString() {
    return "QueryCompilerConfig[allowLeadingWildcards=" + allowLeadingWildcards
        + ", lenient=" + lenient
        + ", trackTotalHits=" + trackTotalHits
        + ", maxResultWindow=" + maxResultWindow
        + ", defaultSize=" + defaultSize
        + ", defaultKeepAlive=" + defaultKeepAlive
        + ", groupNested=" + groupNested
        + ", shardDocTiebreaker=" + shardDocTiebreaker
        + ", allowScriptSort=" + allowScriptSort
        + ", allowRegex=" + allowRegex
        + ", maxPageSize=" + maxPageSize
        + ", maxTermsSize=" + maxTermsSize + "]";
  }

  /** Builds a {@link QueryCompilerConfig}; starts from the {@link #standard()} settings. */
  public static final class Builder {

    private boolean allowLeadingWildcards = false;
    private boolean lenient = false;
    private TrackTotalHits trackTotalHits = TrackTotalHits.exact();
    private int maxResultWindow = 10_000;
    private int defaultSize = 10;
    private Duration defaultKeepAlive = Duration.ofMinutes(1);
    private boolean groupNested = true;
    private boolean shardDocTiebreaker = true;
    private boolean allowScriptSort = false;
    private boolean allowRegex = false;
    private int maxPageSize = DEFAULT_MAX_PAGE_SIZE;
    private int maxTermsSize = DEFAULT_MAX_TERMS_SIZE;

    private Builder() {}

    private Builder(QueryCompilerConfig config) {
      this.allowLeadingWildcards = config.allowLeadingWildcards;
      this.lenient = config.lenient;
      this.trackTotalHits = config.trackTotalHits;
      this.maxResultWindow = config.maxResultWindow;
      this.defaultSize = config.defaultSize;
      this.defaultKeepAlive = config.defaultKeepAlive;
      this.groupNested = config.groupNested;
      this.shardDocTiebreaker = config.shardDocTiebreaker;
      this.allowScriptSort = config.allowScriptSort;
      this.allowRegex = config.allowRegex;
      this.maxPageSize = config.maxPageSize;
      this.maxTermsSize = config.maxTermsSize;
    }

    /** Sets whether wildcard queries may start with {@code *}; default false. */
    public Builder allowLeadingWildcards(boolean value) {
      this.allowLeadingWildcards = value;
      return this;
    }

    /** Sets whether unmapped fields are skipped with a warning; default false. */
    public Builder lenient(boolean value) {
      this.lenient = value;
      return this;
    }

    /**
     * Sets {@code track_total_hits}; default {@link TrackTotalHits#exact()}; {@link
     * TrackTotalHits#engineDefault()} leaves it out of the body.
     *
     * @throws NullPointerException when {@code value} is null
     */
    public Builder trackTotalHits(TrackTotalHits value) {
      this.trackTotalHits = Objects.requireNonNull(value, "trackTotalHits");
      return this;
    }

    /** Sets the largest {@code from + size} without a point in time; default 10000. */
    public Builder maxResultWindow(int value) {
      this.maxResultWindow = value;
      return this;
    }

    /** Sets the page size used when the criteria set none; default 10. */
    public Builder defaultSize(int value) {
      this.defaultSize = value;
      return this;
    }

    /** Sets the point-in-time keep-alive used when the pagination sets none; default 1m. */
    public Builder defaultKeepAlive(Duration value) {
      this.defaultKeepAlive = value;
      return this;
    }

    /** Sets whether predicates on one nested path must match one element; default true. */
    public Builder groupNested(boolean value) {
      this.groupNested = value;
      return this;
    }

    /** Sets whether point-in-time searches get a {@code _shard_doc} tiebreaker; default true. */
    public Builder shardDocTiebreaker(boolean value) {
      this.shardDocTiebreaker = value;
      return this;
    }

    /** Sets whether script sorts are accepted; default false. */
    public Builder allowScriptSort(boolean value) {
      this.allowScriptSort = value;
      return this;
    }

    /** Sets whether {@code TEXT_REGEX} is accepted; default false. */
    public Builder allowRegex(boolean value) {
      this.allowRegex = value;
      return this;
    }

    /** Sets the largest page size; default {@value QueryCompilerConfig#DEFAULT_MAX_PAGE_SIZE}. */
    public Builder maxPageSize(int value) {
      this.maxPageSize = value;
      return this;
    }

    /**
     * Sets the largest {@code size} of a terms aggregation; default {@value
     * QueryCompilerConfig#DEFAULT_MAX_TERMS_SIZE}.
     */
    public Builder maxTermsSize(int value) {
      this.maxTermsSize = value;
      return this;
    }

    /**
     * Builds the settings.
     *
     * @return the settings
     * @throws NullPointerException when {@code defaultKeepAlive} is null
     * @throws IllegalArgumentException when a size, window or keep-alive is out of range
     */
    public QueryCompilerConfig build() {
      return new QueryCompilerConfig(this);
    }
  }
}
