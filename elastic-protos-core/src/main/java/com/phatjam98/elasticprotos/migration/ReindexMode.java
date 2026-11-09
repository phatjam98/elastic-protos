package com.phatjam98.elasticprotos.migration;

/** How {@link IndexMigrator} copies an index for a {@link Action#REINDEX}. */
public enum ReindexMode {
  /**
   * The old index keeps taking writes while it is copied: copy, catch up, swap the alias, freeze
   * the old index, then reconcile the writes made after the copy into the new index with
   * conditional writes. Writes through the alias never fail, except a write that resolved the
   * alias before the swap and reaches the old index after the grace period ({@link
   * MigrationOptions#swapGrace()}). See the README section "Zero-downtime reindex".
   */
  ZERO_DOWNTIME,
  /**
   * The old index is write-blocked for the whole copy, which runs once; writes through the alias
   * fail until the alias has been swapped. The document counts of both indices must match.
   */
  BLOCKING
}
