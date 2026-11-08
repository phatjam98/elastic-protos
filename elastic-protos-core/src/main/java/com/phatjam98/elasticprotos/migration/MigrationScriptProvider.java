package com.phatjam98.elasticprotos.migration;

import java.util.Optional;

/**
 * Supplies the Painless script applied to every document when an index is reindexed from one
 * mapping version to another.
 *
 * <p>Scripts receive the params {@code alias}, {@code from_hash} and {@code to_hash}.
 */
@FunctionalInterface
public interface MigrationScriptProvider {

  /**
   * Returns the script for a migration, if any.
   *
   * @param alias the alias being migrated
   * @param fromHash the hash of the current index, or {@code legacy} when it has none
   * @param toHash the hash of the new index
   * @return the Painless source, or empty for a plain copy
   */
  Optional<String> script(String alias, String fromHash, String toHash);

  /** Returns a provider that never supplies a script. */
  static MigrationScriptProvider none() {
    return (alias, fromHash, toHash) -> Optional.empty();
  }

  /** Returns a provider that reads scripts from {@code migrations/} on the classpath. */
  static MigrationScriptProvider classpath() {
    return new ClasspathScriptProvider();
  }

  /**
   * Returns a provider that reads scripts from a classpath directory.
   *
   * @param prefix the resource directory, for example {@code migrations}
   * @return the provider
   */
  static MigrationScriptProvider classpath(String prefix) {
    return new ClasspathScriptProvider(prefix);
  }
}
