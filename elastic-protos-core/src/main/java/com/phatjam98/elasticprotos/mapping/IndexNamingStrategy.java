package com.phatjam98.elasticprotos.mapping;

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
   * Returns a strategy that prefixes every alias, for example per environment:
   * {@code prefixed("dev")} turns {@code documents} into {@code dev-documents}.
   *
   * <p>The prefix is lower-cased, since index names must be lower case. It must not be empty,
   * must not start with {@code -}, {@code _}, {@code +} or {@code .}, and must not contain
   * whitespace or any of {@code \ / * ? " < > | , # :}, all of which are invalid in index names
   * (a leading {@code .} marks hidden and system indices).
   *
   * @param prefix the prefix without a trailing separator
   * @return the strategy
   * @throws IllegalArgumentException if the prefix cannot start an index name
   */
  static IndexNamingStrategy prefixed(String prefix) {
    String checked = IndexNames.check("index name prefix", prefix);
    return new IndexNamingStrategy() {
      @Override
      public String indexName(String declaredBase, String contentHash) {
        return DEFAULT.indexName(checked + "-" + declaredBase, contentHash);
      }

      @Override
      public String alias(String declaredAlias) {
        return checked + "-" + declaredAlias;
      }
    };
  }
}
