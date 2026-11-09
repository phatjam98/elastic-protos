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
 * @param noops documents a script skipped ({@code ctx.op = 'noop'})
 * @param deleted documents a script deleted ({@code ctx.op = 'delete'})
 * @param versionConflicts documents not copied because of a version conflict
 */
public record ReindexResult(
    long total,
    long created,
    long updated,
    List<Map<String, Object>> failures,
    long tookMs,
    boolean timedOut,
    long noops,
    long deleted,
    long versionConflicts) {

  /**
   * Creates a result without noops, deletions or version conflicts.
   *
   * @param total documents the engine attempted to copy
   * @param created documents created in the destination
   * @param updated documents overwritten in the destination
   * @param failures per-document failures as the engine reported them
   * @param tookMs wall time
   * @param timedOut whether the engine reported a timeout
   */
  public ReindexResult(
      long total,
      long created,
      long updated,
      List<Map<String, Object>> failures,
      long tookMs,
      boolean timedOut) {
    this(total, created, updated, failures, tookMs, timedOut, 0, 0, 0);
  }

  /** Returns whether every document made it across. */
  public boolean ok() {
    return failures.isEmpty() && !timedOut;
  }
}
