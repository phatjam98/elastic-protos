package com.phatjam98.elasticprotos.migration;

/** How {@link IndexMigrator#rollback(IndexPlan, String, RollbackMode)} restores an index. */
public enum RollbackMode {
  /**
   * Copies the writes made to the new index since the reindex back into the previous index with
   * the zero-downtime procedure (copy, catch up, swap, freeze, reconcile, sweep), then retains the
   * new index write-blocked. Any failure before the alias swap aborts the rollback and leaves the
   * alias on the new index.
   */
  REVERSE_DELTA,
  /**
   * Points the alias back at the previous index, unblocks it and write-blocks the new index.
   * Writes made to the new index since the reindex are not carried back.
   */
  ALIAS_ONLY
}
