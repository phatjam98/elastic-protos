package com.phatjam98.elasticprotos.migration;

/**
 * How the reconcile of a zero-downtime reindex or a reverse-delta rollback found what changed in
 * the old index after the copy ({@link ReconcileStats#strategy()}).
 */
public enum ReconcileStrategy {
  /**
   * Only the documents of the frozen old index written after the global checkpoint taken before
   * the copy were read ({@code range} on {@code _seq_no}); every other document was copied
   * unchanged and is in the snapshot as it is. The sweep ran only when document counts showed
   * that the snapshot holds documents the old index no longer has ({@link ReconcileStats#swept()}).
   * Used when the engine reports a global checkpoint ({@link
   * com.phatjam98.elasticprotos.engine.SearchEngine#globalCheckpoint}), the new index was created
   * by the run that copied into it, no migration script applies, the old index does not use
   * segment replication, and {@link MigrationOptions#fullReconcile()} is off.
   */
  SEQ_NO_RANGE,
  /**
   * Every document of the frozen old index was compared with the snapshot, and every document of
   * the snapshot was looked up in the old index (the sweep): the fallback whenever a precondition
   * of {@link #SEQ_NO_RANGE} does not hold, and always for a rollback.
   */
  FULL_SCAN,
  /**
   * The snapshot was lost (its point in time expired), so the indices were compared without it
   * ({@link MigrationOptions#degradedReconcile()}); nothing was swept.
   */
  DEGRADED
}
