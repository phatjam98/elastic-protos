package com.phatjam98.elasticprotos.migration;

/** What the migrator does when static index settings the proto declares differ. */
public enum StaticSettingsPolicy {
  /** Leave them and add a warning; dynamic settings are still applied in place. */
  WARN,
  /**
   * Reindex into the next generation of the index name, created with the new settings (the content
   * hash does not change); {@link Action#BLOCKED} when reindexing is not allowed.
   */
  REINDEX
}
