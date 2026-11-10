package com.phatjam98.elasticprotos.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The highlights of the nested objects of one hit: the fragments of each nested object ({@link
 * #objects()}) and, per nested path and parent object, how many objects matched and how many came
 * back ({@link #totals()}), so a caller can tell when the engine's per-clause limit ({@code
 * Highlight.nested_objects}) cut objects off.
 *
 * <p>Instances are immutable. This is a class rather than a record so that more per-hit inner hits
 * data can be added later without changing {@link Hit}.
 */
public final class NestedHighlights {

  private static final NestedHighlights EMPTY = new NestedHighlights(List.of(), List.of());

  private final List<NestedHighlight> objects;
  private final List<NestedTotal> totals;

  private NestedHighlights(List<NestedHighlight> objects, List<NestedTotal> totals) {
    this.objects = objects == null ? List.of() : List.copyOf(objects);
    this.totals = totals == null ? List.of() : List.copyOf(totals);
  }

  /** Returns the highlights of a hit without nested highlights. */
  public static NestedHighlights empty() {
    return EMPTY;
  }

  /**
   * Returns nested highlights.
   *
   * @param objects one entry per nested object with fragments, ordered by path and offsets; null
   *     means none
   * @param totals one entry per nested path and parent object; null means none
   * @return the highlights
   */
  public static NestedHighlights of(List<NestedHighlight> objects, List<NestedTotal> totals) {
    return (objects == null || objects.isEmpty()) && (totals == null || totals.isEmpty())
        ? EMPTY
        : new NestedHighlights(objects, totals);
  }

  /** Returns the fragments of each nested object that has some, ordered by path and offsets. */
  public List<NestedHighlight> objects() {
    return objects;
  }

  /**
   * Returns how many objects matched and came back, per nested path and parent object, for every
   * level the search asked for inner hits on, including levels without fragments of their own.
   */
  public List<NestedTotal> totals() {
    return totals;
  }

  /** Returns whether the engine left out matching nested objects at some level. */
  public boolean isTruncated() {
    return totals.stream().anyMatch(NestedTotal::truncated);
  }

  /** Returns whether there are neither fragments nor totals. */
  public boolean isEmpty() {
    return objects.isEmpty() && totals.isEmpty();
  }

  /**
   * Returns the fragments of a field in every nested object, in the order of {@link #objects()}.
   *
   * @param field the full dotted index path of the field, such as {@code parties.name}
   * @return the fragments, possibly empty; unmodifiable
   */
  public List<String> fragments(String field) {
    List<String> out = new ArrayList<>();
    for (NestedHighlight object : objects) {
      out.addAll(object.highlight().getOrDefault(field, List.of()));
    }
    return Collections.unmodifiableList(out);
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof NestedHighlights that && objects.equals(that.objects)
        && totals.equals(that.totals);
  }

  @Override
  public int hashCode() {
    return Objects.hash(objects, totals);
  }

  @Override
  public String toString() {
    return "NestedHighlights[objects=" + objects + ", totals=" + totals + "]";
  }
}
