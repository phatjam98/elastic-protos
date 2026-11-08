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
 * @param clearStaleWriteBlock whether a write block found on the index an alias points at is
 *     cleared when that index needs no reindex. A write block on the live alias target is assumed
 *     to be a leftover from an interrupted reindex (the migrator blocks writes to the old index for
 *     the duration of a copy and a killed process never lifts it). Set this to false if you place
 *     write blocks on live indices yourself, or if another process may be reindexing the same
 *     alias at the same time (which is unsupported)
 */
public record MigrationOptions(
    FailurePolicy failure,
    boolean allowReindex,
    boolean deleteOldAfter,
    RemovedFieldPolicy removedFields,
    int defaultShards,
    ReplicaPolicy replicas,
    Duration reindexTimeout,
    boolean dryRun,
    boolean clearStaleWriteBlock) {

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
   * Creates options that clear stale write blocks.
   *
   * @param failure what to do after an index fails to migrate
   * @param allowReindex whether breaking changes may be applied by reindexing
   * @param deleteOldAfter whether the old index is deleted after a successful reindex
   * @param removedFields how fields missing from the proto are treated
   * @param defaultShards shard count for new indices whose settings do not set one
   * @param replicas replica count for new indices whose settings do not set one
   * @param reindexTimeout how long a reindex may take
   * @param dryRun plan only
   */
  public MigrationOptions(
      FailurePolicy failure,
      boolean allowReindex,
      boolean deleteOldAfter,
      RemovedFieldPolicy removedFields,
      int defaultShards,
      ReplicaPolicy replicas,
      Duration reindexTimeout,
      boolean dryRun) {
    this(failure, allowReindex, deleteOldAfter, removedFields, defaultShards, replicas,
        reindexTimeout, dryRun, true);
  }

  /**
   * Returns the defaults: fail fast, reindex allowed, old index retained, removed fields kept, one
   * shard, data nodes minus one replicas (at most 1), a ten minute reindex timeout, no dry run,
   * stale write blocks cleared.
   */
  public static MigrationOptions standard() {
    return new MigrationOptions(FailurePolicy.FAIL_FAST, true, false, RemovedFieldPolicy.KEEP, 1,
        ReplicaPolicy.dataNodesMinusOne(1), Duration.ofMinutes(10), false, true);
  }

  /** Returns a copy with another failure policy. */
  public MigrationOptions withFailure(FailurePolicy value) {
    return new MigrationOptions(value, allowReindex, deleteOldAfter, removedFields, defaultShards,
        replicas, reindexTimeout, dryRun, clearStaleWriteBlock);
  }

  /** Returns a copy that allows or forbids reindexing. */
  public MigrationOptions withAllowReindex(boolean value) {
    return new MigrationOptions(failure, value, deleteOldAfter, removedFields, defaultShards,
        replicas, reindexTimeout, dryRun, clearStaleWriteBlock);
  }

  /** Returns a copy that deletes or retains the old index after a reindex. */
  public MigrationOptions withDeleteOldAfter(boolean value) {
    return new MigrationOptions(failure, allowReindex, value, removedFields, defaultShards,
        replicas, reindexTimeout, dryRun, clearStaleWriteBlock);
  }

  /** Returns a copy with another removed-field policy. */
  public MigrationOptions withRemovedFields(RemovedFieldPolicy value) {
    return new MigrationOptions(failure, allowReindex, deleteOldAfter, value, defaultShards,
        replicas, reindexTimeout, dryRun, clearStaleWriteBlock);
  }

  /** Returns a copy with another default shard count. */
  public MigrationOptions withDefaultShards(int value) {
    return new MigrationOptions(failure, allowReindex, deleteOldAfter, removedFields, value,
        replicas, reindexTimeout, dryRun, clearStaleWriteBlock);
  }

  /** Returns a copy with another replica policy. */
  public MigrationOptions withReplicas(ReplicaPolicy value) {
    return new MigrationOptions(failure, allowReindex, deleteOldAfter, removedFields,
        defaultShards, value, reindexTimeout, dryRun, clearStaleWriteBlock);
  }

  /** Returns a copy with another reindex timeout. */
  public MigrationOptions withReindexTimeout(Duration value) {
    return new MigrationOptions(failure, allowReindex, deleteOldAfter, removedFields,
        defaultShards, replicas, value, dryRun, clearStaleWriteBlock);
  }

  /** Returns a copy that does or does not dry-run. */
  public MigrationOptions withDryRun(boolean value) {
    return new MigrationOptions(failure, allowReindex, deleteOldAfter, removedFields,
        defaultShards, replicas, reindexTimeout, value, clearStaleWriteBlock);
  }

  /** Returns a copy that does or does not clear stale write blocks. */
  public MigrationOptions withClearStaleWriteBlock(boolean value) {
    return new MigrationOptions(failure, allowReindex, deleteOldAfter, removedFields,
        defaultShards, replicas, reindexTimeout, dryRun, value);
  }
}
