package com.phatjam98.elasticprotos.migration;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Reads migration scripts from the classpath. For alias {@code a} migrating from hash {@code f} to
 * hash {@code t} it tries, in order:
 *
 * <ol>
 *   <li>{@code <prefix>/a/f-t.painless}
 *   <li>{@code <prefix>/a/t.painless}
 *   <li>{@code <prefix>/a/default.painless}
 * </ol>
 *
 * <p>{@code f} is {@code legacy} for indices created without elastic-protos metadata. The default
 * prefix is {@code migrations}.
 */
public final class ClasspathScriptProvider implements MigrationScriptProvider {

  /** The default resource directory. */
  public static final String DEFAULT_PREFIX = "migrations";

  private final String prefix;
  private final ClassLoader loader;

  /** Reads from {@code migrations/} with the context class loader. */
  public ClasspathScriptProvider() {
    this(DEFAULT_PREFIX);
  }

  /**
   * Reads from a resource directory with the context class loader.
   *
   * @param prefix the resource directory
   */
  public ClasspathScriptProvider(String prefix) {
    this(prefix, null);
  }

  /**
   * Reads from a resource directory with a given class loader.
   *
   * @param prefix the resource directory
   * @param loader the class loader, or null for the context class loader
   */
  public ClasspathScriptProvider(String prefix, ClassLoader loader) {
    String trimmed = prefix == null ? "" : prefix.replaceAll("^/+|/+$", "");
    this.prefix = trimmed.isEmpty() ? "" : trimmed + "/";
    this.loader = loader;
  }

  /**
   * Returns the resource names tried for a migration, in lookup order.
   *
   * @param alias the alias
   * @param fromHash the current hash, or null for {@code legacy}
   * @param toHash the new hash
   * @return the candidate resource names
   */
  public List<String> candidates(String alias, String fromHash, String toHash) {
    String from = fromHash == null ? "legacy" : fromHash;
    String base = prefix + alias + "/";
    return List.of(
        base + from + "-" + toHash + ".painless",
        base + toHash + ".painless",
        base + "default.painless");
  }

  @Override
  public Optional<String> script(String alias, String fromHash, String toHash) {
    ClassLoader classLoader = loader;
    if (classLoader == null) {
      classLoader = Thread.currentThread().getContextClassLoader();
    }
    if (classLoader == null) {
      classLoader = ClasspathScriptProvider.class.getClassLoader();
    }
    for (String name : candidates(alias, fromHash, toHash)) {
      try (InputStream in = classLoader.getResourceAsStream(name)) {
        if (in != null) {
          return Optional.of(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
      } catch (IOException e) {
        throw new UncheckedIOException("cannot read migration script " + name, e);
      }
    }
    return Optional.empty();
  }
}
