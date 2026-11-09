package com.phatjam98.elasticprotos.engine;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An update by query: rewrites every matching document of an index in place, which re-indexes it
 * under the current mapping and bumps its version and sequence number.
 *
 * <pre>{@code
 * UpdateByQueryRequest request = UpdateByQueryRequest.builder()
 *     .index("documents")
 *     .query(Map.of("ids", Map.of("values", List.of("1", "2"))))
 *     .proceedOnConflicts(true)
 *     .build();
 * }</pre>
 *
 * <p>Settings:
 *
 * <ul>
 *   <li>{@code index}: the index or alias; required
 *   <li>{@code query}: the query selecting the documents; default {@code {"match_all": {}}}; a
 *       shallow copy is kept and returned unmodifiable
 *   <li>{@code proceedOnConflicts}: count documents changed concurrently as version conflicts
 *       instead of aborting ({@code conflicts=proceed}); default false
 *   <li>{@code refresh}: whether to refresh the index when done; default true
 *   <li>{@code timeout}: how long to wait for the task to finish; default ten minutes
 * </ul>
 *
 * <p>Null settings throw {@link NullPointerException}; a timeout that is not positive throws
 * {@link IllegalArgumentException}.
 */
public final class UpdateByQueryRequest {

  private static final Map<String, Object> MATCH_ALL = Map.of("match_all", Map.of());

  private final String index;
  private final Map<String, Object> query;
  private final boolean proceedOnConflicts;
  private final boolean refresh;
  private final Duration timeout;

  private UpdateByQueryRequest(Builder builder) {
    Objects.requireNonNull(builder.index, "index");
    Objects.requireNonNull(builder.query, "query");
    Objects.requireNonNull(builder.timeout, "timeout");
    if (builder.timeout.isNegative() || builder.timeout.isZero()) {
      throw new IllegalArgumentException("timeout must be positive: " + builder.timeout);
    }
    this.index = builder.index;
    this.query = Collections.unmodifiableMap(new LinkedHashMap<>(builder.query));
    this.proceedOnConflicts = builder.proceedOnConflicts;
    this.refresh = builder.refresh;
    this.timeout = builder.timeout;
  }

  /** Updates every document of an index, aborting on conflicts. */
  public static UpdateByQueryRequest of(String index) {
    return builder().index(index).build();
  }

  /** Returns a builder with a match-all query, a final refresh and a ten minute timeout. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns a builder holding this request. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /** Returns the index or alias. */
  public String index() {
    return index;
  }

  /** Returns the query selecting the documents, unmodifiable. */
  public Map<String, Object> query() {
    return query;
  }

  /** Returns whether version conflicts are counted instead of aborting. */
  public boolean proceedOnConflicts() {
    return proceedOnConflicts;
  }

  /** Returns whether the index is refreshed when done. */
  public boolean refresh() {
    return refresh;
  }

  /** Returns how long to wait for the task to finish. */
  public Duration timeout() {
    return timeout;
  }

  /** Returns a copy with another index or alias. */
  public UpdateByQueryRequest withIndex(String value) {
    return toBuilder().index(value).build();
  }

  /** Returns a copy with another query. */
  public UpdateByQueryRequest withQuery(Map<String, Object> value) {
    return toBuilder().query(value).build();
  }

  /** Returns a copy that does or does not proceed on version conflicts. */
  public UpdateByQueryRequest withProceedOnConflicts(boolean value) {
    return toBuilder().proceedOnConflicts(value).build();
  }

  /** Returns a copy that does or does not refresh the index when done. */
  public UpdateByQueryRequest withRefresh(boolean value) {
    return toBuilder().refresh(value).build();
  }

  /** Returns a copy with a timeout. */
  public UpdateByQueryRequest withTimeout(Duration value) {
    return toBuilder().timeout(value).build();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof UpdateByQueryRequest that
        && index.equals(that.index)
        && query.equals(that.query)
        && proceedOnConflicts == that.proceedOnConflicts
        && refresh == that.refresh
        && timeout.equals(that.timeout);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, query, proceedOnConflicts, refresh, timeout);
  }

  @Override
  public String toString() {
    return "UpdateByQueryRequest[index=" + index
        + ", query=" + query
        + ", proceedOnConflicts=" + proceedOnConflicts
        + ", refresh=" + refresh
        + ", timeout=" + timeout + "]";
  }

  /** Builds an {@link UpdateByQueryRequest}. */
  public static final class Builder {

    private String index;
    private Map<String, Object> query = MATCH_ALL;
    private boolean proceedOnConflicts;
    private boolean refresh = true;
    private Duration timeout = Duration.ofMinutes(10);

    private Builder() {}

    private Builder(UpdateByQueryRequest request) {
      this.index = request.index;
      this.query = request.query;
      this.proceedOnConflicts = request.proceedOnConflicts;
      this.refresh = request.refresh;
      this.timeout = request.timeout;
    }

    /** Sets the index or alias. */
    public Builder index(String value) {
      this.index = value;
      return this;
    }

    /** Sets the query; a shallow copy is taken when the request is built. */
    public Builder query(Map<String, Object> value) {
      this.query = value;
      return this;
    }

    /** Sets whether version conflicts are counted instead of aborting; default false. */
    public Builder proceedOnConflicts(boolean value) {
      this.proceedOnConflicts = value;
      return this;
    }

    /** Sets whether the index is refreshed when done; default true. */
    public Builder refresh(boolean value) {
      this.refresh = value;
      return this;
    }

    /** Sets how long to wait for the task to finish; default ten minutes. */
    public Builder timeout(Duration value) {
      this.timeout = value;
      return this;
    }

    /**
     * Builds the request.
     *
     * @return the request
     * @throws NullPointerException when the index, the query or the timeout is null
     * @throws IllegalArgumentException when the timeout is not positive
     */
    public UpdateByQueryRequest build() {
      return new UpdateByQueryRequest(this);
    }
  }
}
