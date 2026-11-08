package com.phatjam98.elasticprotos.migration;

/** How the migrator treats fields that exist in the index but no longer in the proto. */
public enum RemovedFieldPolicy {
  /** Leave them in the mapping and warn; the change is treated as compatible. */
  KEEP,
  /** Reindex into a fresh index without them. */
  REINDEX
}
