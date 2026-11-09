package com.phatjam98.elasticprotos.migration;

import java.util.ArrayList;
import java.util.List;

/**
 * What {@link IndexMigrator#apply(MigrationPlan)} did.
 *
 * @param outcomes one outcome per attempted item, in plan order; with {@link
 *     FailurePolicy#FAIL_FAST} items after the first failure are absent
 */
public record MigrationResult(List<Outcome> outcomes) {

  /** Copies the outcomes. */
  public MigrationResult {
    outcomes = List.copyOf(outcomes);
  }

  /** How an item ended. */
  public enum Status {
    /** The change was made. */
    APPLIED,
    /** Nothing was done because the index was up to date. */
    SKIPPED,
    /** Nothing was done because the plan item is {@link Action#BLOCKED}; see its reason. */
    BLOCKED,
    /** The change failed; see {@link Outcome#error()}. */
    FAILED,
    /**
     * A zero-downtime reindex moved the alias but could not finish its reconcile, because the
     * snapshot of the new index was lost and {@link MigrationOptions#degradedReconcile()} is
     * {@link DegradedReconcile#REPORT}: nothing was written, the warnings count what differs, and
     * the new index keeps its pending marker for the operator to decide.
     */
    INCOMPLETE,
    /**
     * Nothing was done because another process held the alias's migration lock for longer than
     * {@link MigrationOptions#lockWait()}; see {@link Outcome#error()}, a {@link LockException}.
     */
    LOCKED,
    /** Nothing was done because the options ask for a dry run. */
    DRY_RUN
  }

  /**
   * The outcome of one item.
   *
   * @param plan the item
   * @param status how it ended
   * @param index the concrete index behind the alias afterwards (or that would be, for a dry run)
   * @param retainedIndices old indices kept write-blocked after a reindex
   * @param error the failure, or null
   * @param sourceCount for a reindex, the documents counted in the old index after the copy (in a
   *     zero-downtime reindex, once it was frozen); otherwise null
   * @param destCount for a reindex, the documents counted in the new index after the copy (in a
   *     zero-downtime reindex, after the reconcile, including writes made after the swap);
   *     otherwise null
   * @param warnings things that went through but deserve attention, for example a migration
   *     script that left the new index with a different document count
   * @param reconcile for a zero-downtime reindex or a reverse-delta rollback that got past the
   *     alias swap, what the reconcile and the sweep did; otherwise null
   */
  public record Outcome(
      IndexPlan plan, Status status, String index, List<String> retainedIndices,
      Throwable error, Long sourceCount, Long destCount, List<String> warnings,
      ReconcileStats reconcile) {

    /** Copies the lists. */
    public Outcome {
      retainedIndices = List.copyOf(retainedIndices);
      warnings = List.copyOf(warnings);
    }

    /**
     * Creates an outcome without reconcile statistics.
     *
     * @param plan the item
     * @param status how it ended
     * @param index the concrete index behind the alias afterwards
     * @param retainedIndices old indices kept write-blocked after a reindex
     * @param error the failure, or null
     * @param sourceCount documents counted in the old index, or null
     * @param destCount documents counted in the new index, or null
     * @param warnings things that deserve attention
     */
    public Outcome(
        IndexPlan plan, Status status, String index, List<String> retainedIndices,
        Throwable error, Long sourceCount, Long destCount, List<String> warnings) {
      this(plan, status, index, retainedIndices, error, sourceCount, destCount, warnings, null);
    }

    /**
     * Creates an outcome without document counts or warnings.
     *
     * @param plan the item
     * @param status how it ended
     * @param index the concrete index behind the alias afterwards
     * @param retainedIndices old indices kept write-blocked after a reindex
     * @param error the failure, or null
     */
    public Outcome(
        IndexPlan plan, Status status, String index, List<String> retainedIndices,
        Throwable error) {
      this(plan, status, index, retainedIndices, error, null, null, List.of(), null);
    }

    /** Returns whether this item neither failed nor was blocked. */
    public boolean ok() {
      return status != Status.FAILED && status != Status.BLOCKED && status != Status.INCOMPLETE
          && status != Status.LOCKED && plan.action() != Action.BLOCKED;
    }
  }

  /** Returns whether no item failed and none was blocked. */
  public boolean ok() {
    return outcomes.stream().allMatch(Outcome::ok);
  }

  /** Returns the items that failed or were blocked. */
  public List<Outcome> problems() {
    List<Outcome> problems = new ArrayList<>();
    for (Outcome outcome : outcomes) {
      if (!outcome.ok()) {
        problems.add(outcome);
      }
    }
    return problems;
  }
}
