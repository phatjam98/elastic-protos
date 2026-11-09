package com.phatjam98.elasticprotos.engine;

import java.util.List;
import java.util.Map;

/**
 * The outcome of a reindex or an update by query.
 *
 * <p>{@code failures} is copied on construction and returned unmodifiable; the copy is shallow, so
 * the failure maps themselves are shared with the caller.
 *
 * @param total documents the engine attempted to copy or update
 * @param created documents created in the destination
 * @param updated documents overwritten in the destination, or updated in place
 * @param failures per-document failures as the engine reported them
 * @param tookMs wall time
 * @param timedOut whether the engine reported a timeout
 * @param noops documents a script left unchanged
 * @param deleted documents a script deleted
 * @param versionConflicts documents skipped because of a version conflict, counted when the
 *     request proceeds on conflicts
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

  /** Copies the failures. */
  public ReindexResult {
    failures = failures == null ? List.of() : Copies.list(failures);
  }

  /**
   * A result without noops, deletions or version conflicts.
   *
   * @param total documents the engine attempted to copy
   * @param created documents created in the destination
   * @param updated documents overwritten in the destination
   * @param failures per-document failures as the engine reported them
   * @param tookMs wall time
   * @param timedOut whether the engine reported a timeout
   */
  public ReindexResult(long total, long created, long updated, List<Map<String, Object>> failures,
      long tookMs, boolean timedOut) {
    this(total, created, updated, failures, tookMs, timedOut, 0, 0, 0);
  }

  /** Returns whether every document made it across. */
  public boolean ok() {
    return failures.isEmpty() && !timedOut;
  }

  /**
   * Returns whether every document the engine looked at is accounted for: {@code created + updated
   * + deleted + noops + versionConflicts == total}. A run that proceeds on conflicts and is
   * accounted for skipped nothing silently.
   */
  public boolean accounted() {
    return created + updated + deleted + noops + versionConflicts == total;
  }
}
