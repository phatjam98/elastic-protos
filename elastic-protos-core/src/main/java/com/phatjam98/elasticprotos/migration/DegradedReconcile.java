package com.phatjam98.elasticprotos.migration;

/**
 * What a zero-downtime reindex does after the alias swap when the snapshot of the new index (its
 * point in time) is gone, so that writes through the alias after the swap can no longer be told
 * apart from stale copies.
 */
public enum DegradedReconcile {
  /**
   * Write nothing: count the documents missing from the new index, those that differ and those
   * only in the new index, report them as warnings, and end the item {@link
   * MigrationResult.Status#INCOMPLETE} with the pending marker left in place.
   */
  REPORT,
  /**
   * Create the documents the new index lacks and finish. This brings back every document deleted
   * through the alias since the swap; documents that differ are left as the new index has them.
   */
  CREATE_MISSING
}
