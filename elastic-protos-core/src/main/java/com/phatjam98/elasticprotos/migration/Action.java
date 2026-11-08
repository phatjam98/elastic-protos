package com.phatjam98.elasticprotos.migration;

/** What the migrator decided to do for one index. */
public enum Action {
  /** The alias does not exist: create the index with the alias attached. */
  CREATE,
  /** The mappings are up to date: nothing to do. */
  NO_OP,
  /** Only fields were added: extend the mapping of the current index in place. */
  PUT_MAPPING,
  /** A breaking change: create a new index, copy the documents and swap the alias. */
  REINDEX,
  /** The migrator refuses to act; the plan's reason says why. */
  BLOCKED
}
