package com.phatjam98.elasticprotos.engine;

import java.util.List;
import java.util.Objects;

/**
 * How many nested objects at one path, under one parent, matched the query's nested clauses, and
 * how many of them the engine returned. The engines return at most {@code
 * Highlight.nested_objects} objects per clause (3 by default), at every level, so a hit whose
 * matching objects exceed it is truncated; when an outer level is truncated, the deeper objects
 * of the objects left out are missing too.
 *
 * @param parent the levels of the enclosing nested object; empty for objects directly under the
 *     document
 * @param path the full dotted index path of the objects, such as {@code parties} or {@code
 *     boxes.meta.notes}
 * @param matched the number of matching objects reported by the engine: the largest count of one
 *     clause, so a lower bound when several clauses on the path match different objects
 * @param returned the number of distinct objects returned
 * @param truncated whether some clause matched more objects than it returned
 */
public record NestedTotal(List<NestedLevel> parent, String path, long matched, int returned,
    boolean truncated) {

  /** Copies the parent levels; null becomes empty. */
  public NestedTotal {
    parent = parent == null ? List.of() : List.copyOf(parent);
    Objects.requireNonNull(path, "path");
  }
}
