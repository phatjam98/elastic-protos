package com.phatjam98.elasticprotos.mapping;

import java.util.Locale;
import java.util.Objects;

/**
 * Decides alias and concrete index names.
 *
 * <p>The concrete name of a mapping version is {@link #indexName}. That is generation 1. When the
 * migrator must reindex into a new index but the alias already points at an index with that
 * name, it uses a later generation, {@link #generationName}: by default {@code <name>-g2}, {@code
 * -g3} and so on, taking the lowest generation that does not exist yet, or that is a leftover of
 * an interrupted run recording the target hash. This happens because {@code PUT_MAPPING} extends
 * an index in place: the index keeps the name of its first mapping while its recorded hash moves
 * on. Reverting to that first mapping then names the existing index again.
 */
public interface IndexNamingStrategy {

  /** Index name is {@code <alias>-v<hash>}; the alias is used as declared. */
  IndexNamingStrategy DEFAULT = (declaredBase, contentHash) -> declaredBase + "-v" + contentHash;

  /**
   * Returns the concrete index name for a mapping version.
   *
   * @param declaredBase the {@code index} option, or the declared alias when there is none
   * @param contentHash the mapping hash
   * @return the index name
   */
  String indexName(String declaredBase, String contentHash);

  /**
   * Returns the alias to use for a message, given the alias declared by its options or derived
   * from its name.
   *
   * @param declaredAlias the declared alias
   * @return the alias to use
   */
  default String alias(String declaredAlias) {
    return declaredAlias;
  }

  /**
   * Returns the name of a later generation of an index, used when the generation-1 name is taken
   * by the index the alias points at. Implementations must return a distinct name for every
   * generation above 1.
   *
   * @param indexName the generation-1 name, from {@link #indexName}
   * @param generation the generation, 1 or more
   * @return {@code indexName} for generation 1, {@code <indexName>-g<generation>} otherwise
   */
  default String generationName(String indexName, int generation) {
    return generation <= 1 ? indexName : indexName + "-g" + generation;
  }

  /**
   * Returns the name of the index that holds migration locks, one document per alias being
   * migrated, so that concurrent migrations of the same alias exclude each other. Nothing creates
   * it yet: it is reserved for the upcoming zero-downtime reindex. Strategies that separate
   * environments sharing one cluster should return a distinct name per environment, as {@link
   * #prefixed} does.
   *
   * @return {@code elastic-protos-locks} by default
   */
  default String lockIndex() {
    return "elastic-protos-locks";
  }

  /**
   * Returns a strategy that prefixes every alias, for example per environment:
   * {@code prefixed("dev")} turns {@code documents} into {@code dev-documents}, and the lock index
   * into {@code dev-elastic-protos-locks}.
   *
   * <p>The prefix is lower-cased, since index names must be lower case. It must not be empty,
   * must not start with {@code -}, {@code _} or {@code +}, and must not contain whitespace or any
   * of {@code \ / * ? " < > | , # :}, all of which are invalid in index names.
   *
   * @param prefix the prefix without a trailing separator
   * @return the strategy
   * @throws IllegalArgumentException if the prefix cannot start an index name
   */
  static IndexNamingStrategy prefixed(String prefix) {
    String checked = validPrefix(prefix);
    return new IndexNamingStrategy() {
      @Override
      public String indexName(String declaredBase, String contentHash) {
        return DEFAULT.indexName(checked + "-" + declaredBase, contentHash);
      }

      @Override
      public String alias(String declaredAlias) {
        return checked + "-" + declaredAlias;
      }

      @Override
      public String lockIndex() {
        return checked + "-" + IndexNamingStrategy.super.lockIndex();
      }
    };
  }

  private static String validPrefix(String prefix) {
    Objects.requireNonNull(prefix, "prefix");
    String lower = prefix.toLowerCase(Locale.ROOT);
    if (lower.isEmpty()) {
      throw new IllegalArgumentException("index name prefix must not be empty");
    }
    char first = lower.charAt(0);
    if (first == '-' || first == '_' || first == '+') {
      throw new IllegalArgumentException(
          "index name prefix must not start with '" + first + "': " + prefix);
    }
    for (int i = 0; i < lower.length(); i++) {
      char c = lower.charAt(i);
      if (Character.isWhitespace(c) || "\\/*?\"<>|,#:".indexOf(c) >= 0) {
        throw new IllegalArgumentException(
            "index name prefix must not contain '" + c + "': " + prefix);
      }
    }
    return lower;
  }
}
