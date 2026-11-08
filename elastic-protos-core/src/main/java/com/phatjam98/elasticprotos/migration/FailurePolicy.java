package com.phatjam98.elasticprotos.migration;

/** What {@link IndexMigrator#apply(MigrationPlan)} does after an index fails to migrate. */
public enum FailurePolicy {
  /** Stop at the first failure; later indices are neither attempted nor reported. */
  FAIL_FAST,
  /** Attempt every index and report each outcome. */
  CONTINUE
}
