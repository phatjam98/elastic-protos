package com.phatjam98.elasticprotos.migration;

/**
 * A zero-downtime reindex that an earlier run started and did not finish, found while planning in
 * the {@code _meta.elastic_protos.pending} marker of the index it copies into.
 *
 * @param from the index the interrupted run copied from
 * @param pitId the point in time over the new index that the run opened just before the alias
 *     swap, or null when it had not opened one yet
 * @param phase the phase recorded in the marker: {@code copying}, {@code swapping}, {@code
 *     reconciling} or {@code finishing}
 * @param swapped whether the alias already points at the new index. The run then resumes after the
 *     swap (freeze, reconcile, sweep) and never copies with {@code _reindex} again; otherwise it
 *     restarts from the copy, reusing the new index
 * @param rollback whether the interrupted run was a {@link RollbackMode#REVERSE_DELTA} rollback
 */
public record ReindexResume(
    String from, String pitId, String phase, boolean swapped, boolean rollback) {}
