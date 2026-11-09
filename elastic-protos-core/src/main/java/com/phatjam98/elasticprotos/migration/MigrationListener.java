package com.phatjam98.elasticprotos.migration;

/**
 * Hears about the progress of a reindex, a reverse-delta rollback or a backfill, for metrics, logs
 * and tests.
 *
 * <p>{@link #onPhase} runs on the migrating thread right after a phase completed. Exceptions it
 * throws are logged and ignored, except {@link Abort}: that stops the migration where it stands,
 * without any cleanup, as if the process had been killed there. The item is then {@link
 * MigrationResult.Status#FAILED}, and the {@code _meta.elastic_protos.pending} marker lets the next
 * {@link IndexMigrator#plan} pick up the interrupted work. Tests use this to simulate crashes.
 *
 * <p>A {@link ReindexMode#BLOCKING} reindex reports only {@link Phase#COPIED} and {@link
 * Phase#SWAPPED}.
 */
@FunctionalInterface
public interface MigrationListener {

  /** The phases, in the order they complete. */
  enum Phase {
    /** The new index exists and is marked {@code pending.phase = copying}. */
    PREPARED,
    /** The first {@code _reindex} pass finished. */
    COPIED,
    /** The catch-up {@code _reindex} pass finished. */
    CAUGHT_UP,
    /** A point in time over the new index is open and recorded ({@code phase: swapping}). */
    SNAPSHOT,
    /** The alias points at the new index. */
    SWAPPED,
    /** The old index is write-blocked and refreshed ({@code pending.phase = reconciling}). */
    FROZEN,
    /** Documents written to the old index after the copy are in the new index. */
    RECONCILED,
    /** Documents deleted from the old index after the copy are gone from the new index. */
    SWEPT,
    /** The marker was removed; the old index is retained or deleted next. */
    FINISHED,
    /** A backfill put the mapping and marked the index {@code pending.action = backfill}. */
    BACKFILL_MARKED,
    /** The backfill's update by query finished. */
    BACKFILLED,
    /** The backfill marker was removed. */
    BACKFILL_FINISHED
  }

  /**
   * Called after a phase completed.
   *
   * @param alias the alias being migrated
   * @param phase the phase
   */
  void onPhase(String alias, Phase phase);

  /**
   * Thrown by a listener to stop the migration where it stands, without cleanup, as a crash
   * would.
   */
  class Abort extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates an abort.
     *
     * @param message why
     */
    public Abort(String message) {
      super(message);
    }
  }

  /** Returns a listener that does nothing. */
  static MigrationListener noop() {
    return (alias, phase) -> {};
  }
}
