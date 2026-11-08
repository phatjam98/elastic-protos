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
    /** Nothing was done: the index was up to date, or the item was blocked. */
    SKIPPED,
    /** The change failed; see {@link Outcome#error()}. */
    FAILED,
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
   */
  public record Outcome(
      IndexPlan plan, Status status, String index, List<String> retainedIndices,
      Throwable error) {

    /** Copies the retained indices. */
    public Outcome {
      retainedIndices = List.copyOf(retainedIndices);
    }

    /** Returns whether this item neither failed nor was blocked. */
    public boolean ok() {
      return status != Status.FAILED && plan.action() != Action.BLOCKED;
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
