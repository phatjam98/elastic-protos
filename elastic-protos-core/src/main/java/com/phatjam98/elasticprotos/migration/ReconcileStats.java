package com.phatjam98.elasticprotos.migration;

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
 */
public record ReconcileStats(long copied, long superseded, long ghostsDeleted, long staleSkipped) {}
