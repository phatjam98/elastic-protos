package com.phatjam98.elasticprotos.migration;

import java.time.Duration;
import java.util.Objects;

/**
 * Settings for {@link IndexMigrator}.
 *
 * <p>Start from {@link #standard()} and change single settings with the {@code with*} methods, or
 * set several at once with {@link #builder()} or {@link #toBuilder()}:
 *
 * <pre>{@code
 * MigrationOptions options = MigrationOptions.builder()
 *     .failure(FailurePolicy.CONTINUE)
 *     .replicas(ReplicaPolicy.fixed(1))
 *     .reindexTimeout(Duration.ofMinutes(30))
 *     .build();
 * }</pre>
 *
 * <p>Settings:
 *
 * <ul>
 *   <li>{@code failure}: what to do after an index fails to migrate; default {@link
 *       FailurePolicy#FAIL_FAST}
 *   <li>{@code allowReindex}: whether breaking changes may be applied by reindexing; when false
 *       they are {@link Action#BLOCKED}; default true
 *   <li>{@code deleteOldAfter}: whether the old index is deleted after a successful reindex
 *       instead of being retained write-blocked; default false
 *   <li>{@code removedFields}: how fields missing from the proto are treated; default {@link
 *       RemovedFieldPolicy#KEEP}
 *   <li>{@code defaultShards}: shard count for new indices whose settings do not set one; default 1
 *   <li>{@code replicas}: replica count for new indices whose settings do not set one; default
 *       {@link ReplicaPolicy#dataNodesMinusOne(int) dataNodesMinusOne(1)}
 *   <li>{@code reindexTimeout}: how long a reindex may take; default ten minutes
 *   <li>{@code dryRun}: plan only; {@link IndexMigrator#apply(MigrationPlan)} changes nothing;
 *       default false
 *   <li>{@code clearStaleWriteBlock}: whether a write block found on the index an alias points at
 *       is cleared when that index needs no reindex; default true. A write block on the live alias
 *       target is assumed to be a leftover from an interrupted reindex (the migrator blocks writes
 *       to the old index for the duration of a copy and a killed process never lifts it). Set this
 *       to false if you place write blocks on live indices yourself, or if another process may be
 *       reindexing the same alias at the same time (which is unsupported)
 *   <li>{@code reindexMode}: how a reindex copies the old index; default {@link
 *       ReindexMode#ZERO_DOWNTIME}, which never blocks writes to the live index; {@link
 *       ReindexMode#BLOCKING} write-blocks it for the whole copy
 *   <li>{@code swapGrace}: in a zero-downtime reindex, how long to wait after the alias swap
 *       before the old index is write-blocked, so that writes that resolved the alias just before
 *       the swap still land; default five seconds, zero or more
 *   <li>{@code backfill}: whether additions that existing documents were never indexed into are
 *       applied in place with an update by query ({@link Action#BACKFILL}); when false they are
 *       reindexed; default true
 * </ul>
 *
 * <p>Null settings throw {@link NullPointerException}; a shard count below 1 or a timeout that is
 * not positive, or a negative swap grace, throws {@link IllegalArgumentException}.
 */
public final class MigrationOptions {

  private final FailurePolicy failure;
  private final boolean allowReindex;
  private final boolean deleteOldAfter;
  private final RemovedFieldPolicy removedFields;
  private final int defaultShards;
  private final ReplicaPolicy replicas;
  private final Duration reindexTimeout;
  private final boolean dryRun;
  private final boolean clearStaleWriteBlock;
  private final ReindexMode reindexMode;
  private final Duration swapGrace;
  private final boolean backfill;

  private MigrationOptions(Builder builder) {
    Objects.requireNonNull(builder.failure, "failure");
    Objects.requireNonNull(builder.removedFields, "removedFields");
    Objects.requireNonNull(builder.replicas, "replicas");
    Objects.requireNonNull(builder.reindexTimeout, "reindexTimeout");
    Objects.requireNonNull(builder.reindexMode, "reindexMode");
    Objects.requireNonNull(builder.swapGrace, "swapGrace");
    if (builder.swapGrace.isNegative()) {
      throw new IllegalArgumentException("swapGrace must not be negative: " + builder.swapGrace);
    }
    if (builder.reindexTimeout.isNegative() || builder.reindexTimeout.isZero()) {
      throw new IllegalArgumentException(
          "reindexTimeout must be positive: " + builder.reindexTimeout);
    }
    if (builder.defaultShards < 1) {
      throw new IllegalArgumentException(
          "defaultShards must be at least 1: " + builder.defaultShards);
    }
    this.failure = builder.failure;
    this.allowReindex = builder.allowReindex;
    this.deleteOldAfter = builder.deleteOldAfter;
    this.removedFields = builder.removedFields;
    this.defaultShards = builder.defaultShards;
    this.replicas = builder.replicas;
    this.reindexTimeout = builder.reindexTimeout;
    this.dryRun = builder.dryRun;
    this.clearStaleWriteBlock = builder.clearStaleWriteBlock;
    this.reindexMode = builder.reindexMode;
    this.swapGrace = builder.swapGrace;
    this.backfill = builder.backfill;
  }

  /**
   * Returns the defaults: fail fast, reindex allowed, old index retained, removed fields kept, one
   * shard, data nodes minus one replicas (at most 1), a ten minute reindex timeout, no dry run,
   * stale write blocks cleared, zero-downtime reindexing with a five second swap grace, backfill
   * enabled.
   */
  public static MigrationOptions standard() {
    return builder().build();
  }

  /** Returns a builder holding the {@link #standard()} options. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns a builder holding these options. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /** Returns what to do after an index fails to migrate. */
  public FailurePolicy failure() {
    return failure;
  }

  /** Returns whether breaking changes may be applied by reindexing. */
  public boolean allowReindex() {
    return allowReindex;
  }

  /** Returns whether the old index is deleted after a successful reindex. */
  public boolean deleteOldAfter() {
    return deleteOldAfter;
  }

  /** Returns how fields missing from the proto are treated. */
  public RemovedFieldPolicy removedFields() {
    return removedFields;
  }

  /** Returns the shard count for new indices whose settings do not set one. */
  public int defaultShards() {
    return defaultShards;
  }

  /** Returns the replica policy for new indices whose settings do not set a count. */
  public ReplicaPolicy replicas() {
    return replicas;
  }

  /** Returns how long a reindex may take. */
  public Duration reindexTimeout() {
    return reindexTimeout;
  }

  /** Returns whether {@link IndexMigrator#apply(MigrationPlan)} only reports. */
  public boolean dryRun() {
    return dryRun;
  }

  /** Returns whether a stale write block on the alias target is cleared. */
  public boolean clearStaleWriteBlock() {
    return clearStaleWriteBlock;
  }

  /** Returns how a reindex copies the old index. */
  public ReindexMode reindexMode() {
    return reindexMode;
  }

  /** Returns how long a zero-downtime reindex waits after the swap to freeze the old index. */
  public Duration swapGrace() {
    return swapGrace;
  }

  /** Returns whether unindexed additions are backfilled in place instead of reindexed. */
  public boolean backfill() {
    return backfill;
  }

  /** Returns a copy with another failure policy. */
  public MigrationOptions withFailure(FailurePolicy value) {
    return toBuilder().failure(value).build();
  }

  /** Returns a copy that allows or forbids reindexing. */
  public MigrationOptions withAllowReindex(boolean value) {
    return toBuilder().allowReindex(value).build();
  }

  /** Returns a copy that deletes or retains the old index after a reindex. */
  public MigrationOptions withDeleteOldAfter(boolean value) {
    return toBuilder().deleteOldAfter(value).build();
  }

  /** Returns a copy with another removed-field policy. */
  public MigrationOptions withRemovedFields(RemovedFieldPolicy value) {
    return toBuilder().removedFields(value).build();
  }

  /** Returns a copy with another default shard count. */
  public MigrationOptions withDefaultShards(int value) {
    return toBuilder().defaultShards(value).build();
  }

  /** Returns a copy with another replica policy. */
  public MigrationOptions withReplicas(ReplicaPolicy value) {
    return toBuilder().replicas(value).build();
  }

  /** Returns a copy with another reindex timeout. */
  public MigrationOptions withReindexTimeout(Duration value) {
    return toBuilder().reindexTimeout(value).build();
  }

  /** Returns a copy that does or does not dry-run. */
  public MigrationOptions withDryRun(boolean value) {
    return toBuilder().dryRun(value).build();
  }

  /** Returns a copy that does or does not clear stale write blocks. */
  public MigrationOptions withClearStaleWriteBlock(boolean value) {
    return toBuilder().clearStaleWriteBlock(value).build();
  }

  /** Returns a copy with another reindex mode. */
  public MigrationOptions withReindexMode(ReindexMode value) {
    return toBuilder().reindexMode(value).build();
  }

  /** Returns a copy with another swap grace. */
  public MigrationOptions withSwapGrace(Duration value) {
    return toBuilder().swapGrace(value).build();
  }

  /** Returns a copy that backfills unindexed additions in place, or reindexes them. */
  public MigrationOptions withBackfill(boolean value) {
    return toBuilder().backfill(value).build();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof MigrationOptions that
        && failure == that.failure
        && allowReindex == that.allowReindex
        && deleteOldAfter == that.deleteOldAfter
        && removedFields == that.removedFields
        && defaultShards == that.defaultShards
        && replicas.equals(that.replicas)
        && reindexTimeout.equals(that.reindexTimeout)
        && dryRun == that.dryRun
        && clearStaleWriteBlock == that.clearStaleWriteBlock
        && reindexMode == that.reindexMode
        && swapGrace.equals(that.swapGrace)
        && backfill == that.backfill;
  }

  @Override
  public int hashCode() {
    return Objects.hash(failure, allowReindex, deleteOldAfter, removedFields, defaultShards,
        replicas, reindexTimeout, dryRun, clearStaleWriteBlock, reindexMode, swapGrace, backfill);
  }

  @Override
  public String toString() {
    return "MigrationOptions[failure=" + failure
        + ", allowReindex=" + allowReindex
        + ", deleteOldAfter=" + deleteOldAfter
        + ", removedFields=" + removedFields
        + ", defaultShards=" + defaultShards
        + ", replicas=" + replicas
        + ", reindexTimeout=" + reindexTimeout
        + ", dryRun=" + dryRun
        + ", clearStaleWriteBlock=" + clearStaleWriteBlock
        + ", reindexMode=" + reindexMode
        + ", swapGrace=" + swapGrace
        + ", backfill=" + backfill + "]";
  }

  /** Builds {@link MigrationOptions}; starts from the {@link #standard()} options. */
  public static final class Builder {

    private FailurePolicy failure = FailurePolicy.FAIL_FAST;
    private boolean allowReindex = true;
    private boolean deleteOldAfter = false;
    private RemovedFieldPolicy removedFields = RemovedFieldPolicy.KEEP;
    private int defaultShards = 1;
    private ReplicaPolicy replicas = ReplicaPolicy.dataNodesMinusOne(1);
    private Duration reindexTimeout = Duration.ofMinutes(10);
    private boolean dryRun = false;
    private boolean clearStaleWriteBlock = true;
    private ReindexMode reindexMode = ReindexMode.ZERO_DOWNTIME;
    private Duration swapGrace = Duration.ofSeconds(5);
    private boolean backfill = true;

    private Builder() {}

    private Builder(MigrationOptions options) {
      this.failure = options.failure;
      this.allowReindex = options.allowReindex;
      this.deleteOldAfter = options.deleteOldAfter;
      this.removedFields = options.removedFields;
      this.defaultShards = options.defaultShards;
      this.replicas = options.replicas;
      this.reindexTimeout = options.reindexTimeout;
      this.dryRun = options.dryRun;
      this.clearStaleWriteBlock = options.clearStaleWriteBlock;
      this.reindexMode = options.reindexMode;
      this.swapGrace = options.swapGrace;
      this.backfill = options.backfill;
    }

    /** Sets what to do after an index fails to migrate; default FAIL_FAST. */
    public Builder failure(FailurePolicy value) {
      this.failure = value;
      return this;
    }

    /** Sets whether breaking changes may be applied by reindexing; default true. */
    public Builder allowReindex(boolean value) {
      this.allowReindex = value;
      return this;
    }

    /** Sets whether the old index is deleted after a successful reindex; default false. */
    public Builder deleteOldAfter(boolean value) {
      this.deleteOldAfter = value;
      return this;
    }

    /** Sets how fields missing from the proto are treated; default KEEP. */
    public Builder removedFields(RemovedFieldPolicy value) {
      this.removedFields = value;
      return this;
    }

    /** Sets the shard count for new indices whose settings do not set one; default 1. */
    public Builder defaultShards(int value) {
      this.defaultShards = value;
      return this;
    }

    /** Sets the replica policy for new indices; default data nodes minus one, at most 1. */
    public Builder replicas(ReplicaPolicy value) {
      this.replicas = value;
      return this;
    }

    /** Sets how long a reindex may take; default ten minutes. */
    public Builder reindexTimeout(Duration value) {
      this.reindexTimeout = value;
      return this;
    }

    /** Sets whether {@link IndexMigrator#apply(MigrationPlan)} only reports; default false. */
    public Builder dryRun(boolean value) {
      this.dryRun = value;
      return this;
    }

    /** Sets whether a stale write block on the alias target is cleared; default true. */
    public Builder clearStaleWriteBlock(boolean value) {
      this.clearStaleWriteBlock = value;
      return this;
    }

    /** Sets how a reindex copies the old index; default ZERO_DOWNTIME. */
    public Builder reindexMode(ReindexMode value) {
      this.reindexMode = value;
      return this;
    }

    /**
     * Sets how long a zero-downtime reindex waits after the alias swap before it write-blocks the
     * old index; default five seconds.
     */
    public Builder swapGrace(Duration value) {
      this.swapGrace = value;
      return this;
    }

    /** Sets whether unindexed additions are backfilled in place; default true. */
    public Builder backfill(boolean value) {
      this.backfill = value;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     * @throws NullPointerException when a policy, the mode, the timeout or the grace is null
     * @throws IllegalArgumentException when {@code defaultShards} is below 1, the timeout is not
     *     positive or the grace is negative
     */
    public MigrationOptions build() {
      return new MigrationOptions(this);
    }
  }
}
