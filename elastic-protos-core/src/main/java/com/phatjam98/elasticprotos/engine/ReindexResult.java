package com.phatjam98.elasticprotos.engine;

import java.util.List;
import java.util.Map;

/**
 * The outcome of a reindex.
 *
 * @param total documents the engine attempted to copy
 * @param created documents created in the destination
 * @param updated documents overwritten in the destination
 * @param failures per-document failures as the engine reported them
 * @param tookMs wall time
 * @param timedOut whether the engine reported a timeout
 */
public record ReindexResult(
    long total,
    long created,
    long updated,
    List<Map<String, Object>> failures,
    long tookMs,
    boolean timedOut) {

  /** Returns whether every document made it across. */
  public boolean ok() {
    return failures.isEmpty() && !timedOut;
  }
}
