package com.phatjam98.elasticprotos.engine;

import java.time.Duration;
import java.util.Map;

/**
 * A reindex from one index to another.
 *
 * @param source the source index or alias
 * @param dest the destination index
 * @param script an optional Painless script applied to every document, or null
 * @param scriptParams parameters for the script
 * @param timeout how long to wait for the reindex task to finish
 * @param refresh whether to refresh the destination when done
 */
public record ReindexRequest(
    String source,
    String dest,
    String script,
    Map<String, Object> scriptParams,
    Duration timeout,
    boolean refresh) {

  /** A plain reindex with a ten minute timeout and a final refresh. */
  public static ReindexRequest of(String source, String dest) {
    return new ReindexRequest(source, dest, null, Map.of(), Duration.ofMinutes(10), true);
  }

  /** Returns a copy with a script. */
  public ReindexRequest withScript(String painless, Map<String, Object> params) {
    return new ReindexRequest(source, dest, painless, Map.copyOf(params), timeout, refresh);
  }

  /** Returns a copy with a timeout. */
  public ReindexRequest withTimeout(Duration value) {
    return new ReindexRequest(source, dest, script, scriptParams, value, refresh);
  }
}
