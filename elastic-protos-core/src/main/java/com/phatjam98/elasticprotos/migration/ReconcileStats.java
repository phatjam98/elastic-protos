package com.phatjam98.elasticprotos.migration;

import java.util.Objects;

/**
 * What the reconcile and the sweep of a zero-downtime reindex did after the alias swap.
 *
 * @param copied documents written into the new index: created there, or overwritten because the
 *     old index held a later write than the copy
 * @param superseded documents left alone because a write through the alias after the swap won: a
 *     conditional write or delete found a newer document (409) or none (404)
 * @param ghostsDeleted documents deleted from the new index because they had been deleted from the
 *     old index after the copy
 * @param staleSkipped documents that differ between the indices but were left as they are in the
 *     new index, because the snapshot of the new index taken at the swap was lost (its point in
 *     time expired), so a write after the swap could not be told apart from a stale copy
 * @param strategy how the changed documents were found
 * @param swept whether the snapshot was swept for documents deleted from the old index; with
 *     {@link ReconcileStrategy#SEQ_NO_RANGE} it is skipped when the document counts prove that
 *     there are none
 */
public record ReconcileStats(long copied, long superseded, long ghostsDeleted, long staleSkipped,
    ReconcileStrategy strategy, boolean swept) {

  /** Requires a strategy. */
  public ReconcileStats {
    Objects.requireNonNull(strategy, "strategy");
  }

  /**
   * Creates the statistics of a {@link ReconcileStrategy#FULL_SCAN} reconcile that swept.
   *
   * @param copied documents written into the new index
   * @param superseded documents left alone because a write after the swap won
   * @param ghostsDeleted documents deleted from the new index
   * @param staleSkipped documents that differ but were left alone without the snapshot
   */
  public ReconcileStats(long copied, long superseded, long ghostsDeleted, long staleSkipped) {
    this(copied, superseded, ghostsDeleted, staleSkipped, ReconcileStrategy.FULL_SCAN, true);
  }
}
