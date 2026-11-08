package com.phatjam98.elasticprotos.engine;

import java.util.List;

/**
 * One step of an atomic alias update.
 *
 * @param type what to do
 * @param indices the indices the action applies to
 * @param alias the alias, or null for {@code REMOVE_INDEX}
 */
public record AliasAction(Type type, List<String> indices, String alias) {

  /** The kinds of alias action. */
  public enum Type {
    ADD,
    REMOVE,
    REMOVE_INDEX
  }

  /** Points an alias at an index. */
  public static AliasAction add(String index, String alias) {
    return new AliasAction(Type.ADD, List.of(index), alias);
  }

  /** Removes an alias from an index. */
  public static AliasAction remove(String index, String alias) {
    return new AliasAction(Type.REMOVE, List.of(index), alias);
  }

  /** Removes an alias from several indices. */
  public static AliasAction remove(List<String> indices, String alias) {
    return new AliasAction(Type.REMOVE, List.copyOf(indices), alias);
  }

  /** Deletes an index as part of the same atomic update. */
  public static AliasAction removeIndex(String index) {
    return new AliasAction(Type.REMOVE_INDEX, List.of(index), null);
  }
}
