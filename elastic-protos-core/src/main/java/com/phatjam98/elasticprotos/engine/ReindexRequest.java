package com.phatjam98.elasticprotos.engine;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A reindex from one index to another.
 *
 * <p>Create one with {@link #of(String, String)} and the {@code with*} methods, or with {@link
 * #builder()}:
 *
 * <pre>{@code
 * ReindexRequest request = ReindexRequest.builder()
 *     .source("documents-v1")
 *     .dest("documents-v2")
 *     .script("ctx._source.tag = params.tag", Map.of("tag", "migrated"))
 *     .timeout(Duration.ofMinutes(30))
 *     .build();
 * }</pre>
 *
 * <p>Settings:
 *
 * <ul>
 *   <li>{@code source}: the source index or alias; required
 *   <li>{@code dest}: the destination index; required
 *   <li>{@code script}: an optional Painless script applied to every document, or null
 *   <li>{@code scriptParams}: parameters for the script; copied one level deep and returned
 *       unmodifiable; values may be null
 *   <li>{@code timeout}: how long to wait for the reindex task to finish; default ten minutes
 *   <li>{@code refresh}: whether to refresh the destination when done; default true
 * </ul>
 */
public final class ReindexRequest {

  private final String source;
  private final String dest;
  private final String script;
  private final Map<String, Object> scriptParams;
  private final Duration timeout;
  private final boolean refresh;

  private ReindexRequest(Builder builder) {
    if (builder.source == null) {
      throw new IllegalArgumentException("source is required");
    }
    if (builder.dest == null) {
      throw new IllegalArgumentException("dest is required");
    }
    this.source = builder.source;
    this.dest = builder.dest;
    this.script = builder.script;
    this.scriptParams = builder.scriptParams == null
        ? Map.of()
        : Collections.unmodifiableMap(new LinkedHashMap<>(builder.scriptParams));
    this.timeout = builder.timeout;
    this.refresh = builder.refresh;
  }

  /** A plain reindex with a ten minute timeout and a final refresh. */
  public static ReindexRequest of(String source, String dest) {
    return builder().source(source).dest(dest).build();
  }

  /**
   * Returns a builder with a ten minute timeout, a final refresh and no script; source and dest
   * must be set.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns a builder holding this request. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /** Returns the source index or alias. */
  public String source() {
    return source;
  }

  /** Returns the destination index. */
  public String dest() {
    return dest;
  }

  /** Returns the Painless script applied to every document, or null. */
  public String script() {
    return script;
  }

  /** Returns the script parameters, unmodifiable and possibly empty. */
  public Map<String, Object> scriptParams() {
    return scriptParams;
  }

  /** Returns how long to wait for the reindex task to finish. */
  public Duration timeout() {
    return timeout;
  }

  /** Returns whether the destination is refreshed when done. */
  public boolean refresh() {
    return refresh;
  }

  /** Returns a copy with a script. */
  public ReindexRequest withScript(String painless, Map<String, Object> params) {
    return toBuilder().script(painless, params).build();
  }

  /** Returns a copy with a timeout. */
  public ReindexRequest withTimeout(Duration value) {
    return toBuilder().timeout(value).build();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof ReindexRequest that
        && source.equals(that.source)
        && dest.equals(that.dest)
        && Objects.equals(script, that.script)
        && scriptParams.equals(that.scriptParams)
        && Objects.equals(timeout, that.timeout)
        && refresh == that.refresh;
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, dest, script, scriptParams, timeout, refresh);
  }

  @Override
  public String toString() {
    return "ReindexRequest[source=" + source
        + ", dest=" + dest
        + ", script=" + script
        + ", scriptParams=" + scriptParams
        + ", timeout=" + timeout
        + ", refresh=" + refresh + "]";
  }

  /** Builds a {@link ReindexRequest}. */
  public static final class Builder {

    private String source;
    private String dest;
    private String script;
    private Map<String, Object> scriptParams = Map.of();
    private Duration timeout = Duration.ofMinutes(10);
    private boolean refresh = true;

    private Builder() {}

    private Builder(ReindexRequest request) {
      this.source = request.source;
      this.dest = request.dest;
      this.script = request.script;
      this.scriptParams = request.scriptParams;
      this.timeout = request.timeout;
      this.refresh = request.refresh;
    }

    /** Sets the source index or alias. */
    public Builder source(String value) {
      this.source = value;
      return this;
    }

    /** Sets the destination index. */
    public Builder dest(String value) {
      this.dest = value;
      return this;
    }

    /** Sets the Painless script applied to every document; null for none. */
    public Builder script(String value) {
      this.script = value;
      return this;
    }

    /** Sets the script and its parameters. */
    public Builder script(String painless, Map<String, Object> params) {
      return script(painless).scriptParams(params);
    }

    /** Sets the script parameters; they are copied when the request is built. */
    public Builder scriptParams(Map<String, Object> value) {
      this.scriptParams = value;
      return this;
    }

    /** Sets how long to wait for the reindex task to finish; default ten minutes. */
    public Builder timeout(Duration value) {
      this.timeout = value;
      return this;
    }

    /** Sets whether the destination is refreshed when done; default true. */
    public Builder refresh(boolean value) {
      this.refresh = value;
      return this;
    }

    /**
     * Builds the request.
     *
     * @return the request
     * @throws IllegalArgumentException when source or dest is missing
     */
    public ReindexRequest build() {
      return new ReindexRequest(this);
    }
  }
}
