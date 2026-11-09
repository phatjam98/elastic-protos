package com.phatjam98.elasticprotos.migration;

/** What the migrator decided to do for one index. */
public enum Action {
  /** The alias does not exist: create the index with the alias attached. */
  CREATE,
  /** The mappings are up to date: nothing to do. */
  NO_OP,
  /** Only fields were added: extend the mapping of the current index in place. */
  PUT_MAPPING,
  /**
   * Only dynamic index settings the proto declares differ: update them in place with {@code PUT
   * /<index>/_settings}.
   */
  UPDATE_SETTINGS,
  /**
   * Fields were added that existing documents were never indexed into (a multi-field under an
   * existing field, or a typed property under a dynamic object): extend the mapping in place, then
   * re-index every document of the current index into it with an update by query.
   */
  BACKFILL,
  /** A breaking change: create a new index, copy the documents and swap the alias. */
  REINDEX,
  /** The migrator refuses to act; the plan's reason says why. */
  BLOCKED
}
