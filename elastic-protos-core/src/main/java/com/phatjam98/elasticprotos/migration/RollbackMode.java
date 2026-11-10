package com.phatjam98.elasticprotos.migration;

/** How {@link IndexMigrator#rollback(IndexPlan, String, RollbackMode)} restores an index. */
public enum RollbackMode {
  /**
   * Copies the writes made to the new index since the reindex back into the previous index with
   * the zero-downtime procedure (copy, catch up, swap, freeze, reconcile, sweep), then retains the
   * new index write-blocked. Any failure before the alias swap aborts the rollback and leaves the
   * alias on the new index, though the previous index may already hold some of the copied
   * documents. Refused (the outcome is FAILED) when a migration script exists for the reindex being
   * rolled back, since its transformed documents would be copied back; see {@link
   * #REVERSE_DELTA_FORCE}.
   */
  REVERSE_DELTA,
  /** {@link #REVERSE_DELTA} even when a migration script exists for the reindex. */
  REVERSE_DELTA_FORCE,
  /**
   * Points the alias back at the previous index, unblocks it and write-blocks the new index.
   * Writes made to the new index since the reindex are not carried back.
   */
  ALIAS_ONLY
}
