package com.phatjam98.elasticprotos.migration;

import java.time.Duration;

/**
 * Settings for {@link IndexMigrator}.
 *
 * @param failure what to do after an index fails to migrate
 * @param allowReindex whether breaking changes may be applied by reindexing; when false they are
 *     {@link Action#BLOCKED}
 * @param deleteOldAfter whether the old index is deleted after a successful reindex instead of
 *     being retained write-blocked
 * @param removedFields how fields missing from the proto are treated
 * @param defaultShards shard count for new indices whose settings do not set one
 * @param replicas replica count for new indices whose settings do not set one
 * @param reindexTimeout how long a reindex may take
 * @param dryRun plan only; {@link IndexMigrator#apply(MigrationPlan)} changes nothing
 */
public record MigrationOptions(
    FailurePolicy failure,
    boolean allowReindex,
    boolean deleteOldAfter,
    RemovedFieldPolicy removedFields,
    int defaultShards,
    ReplicaPolicy replicas,
    Duration reindexTimeout,
    boolean dryRun) {

  /** Validates the options. */
  public MigrationOptions {
    if (failure == null || removedFields == null || replicas == null || reindexTimeout == null) {
      throw new IllegalArgumentException("failure, removedFields, replicas and reindexTimeout"
          + " are required");
    }
    if (defaultShards < 1) {
      throw new IllegalArgumentException("defaultShards must be at least 1: " + defaultShards);
    }
  }

  /**
   * Returns the defaults: fail fast, reindex allowed, old index retained, removed fields kept, one
   * shard, data nodes minus one replicas (at most 1), a ten minute reindex timeout, no dry run.
   */
  public static MigrationOptions standard() {
    return new MigrationOptions(FailurePolicy.FAIL_FAST, true, false, RemovedFieldPolicy.KEEP, 1,
        ReplicaPolicy.dataNodesMinusOne(1), Duration.ofMinutes(10), false);
  }

  /** Returns a copy with another failure policy. */
  public MigrationOptions withFailure(FailurePolicy value) {
    return new MigrationOptions(value, allowReindex, deleteOldAfter, removedFields, defaultShards,
        replicas, reindexTimeout, dryRun);
  }

  /** Returns a copy that allows or forbids reindexing. */
  public MigrationOptions withAllowReindex(boolean value) {
    return new MigrationOptions(failure, value, deleteOldAfter, removedFields, defaultShards,
        replicas, reindexTimeout, dryRun);
  }

  /** Returns a copy that deletes or retains the old index after a reindex. */
  public MigrationOptions withDeleteOldAfter(boolean value) {
    return new MigrationOptions(failure, allowReindex, value, removedFields, defaultShards,
        replicas, reindexTimeout, dryRun);
  }

  /** Returns a copy with another removed-field policy. */
  public MigrationOptions withRemovedFields(RemovedFieldPolicy value) {
    return new MigrationOptions(failure, allowReindex, deleteOldAfter, value, defaultShards,
        replicas, reindexTimeout, dryRun);
  }

  /** Returns a copy with another default shard count. */
  public MigrationOptions withDefaultShards(int value) {
    return new MigrationOptions(failure, allowReindex, deleteOldAfter, removedFields, value,
        replicas, reindexTimeout, dryRun);
  }

  /** Returns a copy with another replica policy. */
  public MigrationOptions withReplicas(ReplicaPolicy value) {
    return new MigrationOptions(failure, allowReindex, deleteOldAfter, removedFields,
        defaultShards, value, reindexTimeout, dryRun);
  }

  /** Returns a copy with another reindex timeout. */
  public MigrationOptions withReindexTimeout(Duration value) {
    return new MigrationOptions(failure, allowReindex, deleteOldAfter, removedFields,
        defaultShards, replicas, value, dryRun);
  }

  /** Returns a copy that does or does not dry-run. */
  public MigrationOptions withDryRun(boolean value) {
    return new MigrationOptions(failure, allowReindex, deleteOldAfter, removedFields,
        defaultShards, replicas, reindexTimeout, value);
  }
}
