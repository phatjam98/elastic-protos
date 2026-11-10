package com.phatjam98.elasticprotos.engine;

import java.util.Objects;

/**
 * One level of the position of a nested object inside a hit, as the engines report it in {@code
 * _nested}: the field of the nested array, relative to the enclosing level, and the position in
 * that array.
 *
 * <p>The field is relative and may hold dots: a nested {@code notes} inside a plain object {@code
 * meta} of a nested {@code boxes} is reported as the levels {@code boxes} and {@code meta.notes}.
 *
 * @param field the index path of the nested array relative to the enclosing level (the document
 *     for the first level), such as {@code parties} or {@code meta.notes}
 * @param offset the position of the object in that array, counting from 0
 */
public record NestedLevel(String field, int offset) {

  /** Checks the field. */
  public NestedLevel {
    Objects.requireNonNull(field, "field");
  }
}
