package com.phatjam98.elasticprotos.mapping;

/** Decides alias and concrete index names. */
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
   * Returns a strategy that prefixes every alias, for example per environment:
   * {@code prefixed("dev")} turns {@code documents} into {@code dev-documents}.
   *
   * @param prefix the prefix without a trailing separator
   * @return the strategy
   */
  static IndexNamingStrategy prefixed(String prefix) {
    return new IndexNamingStrategy() {
      @Override
      public String indexName(String declaredBase, String contentHash) {
        return DEFAULT.indexName(prefix + "-" + declaredBase, contentHash);
      }

      @Override
      public String alias(String declaredAlias) {
        return prefix + "-" + declaredAlias;
      }
    };
  }
}
