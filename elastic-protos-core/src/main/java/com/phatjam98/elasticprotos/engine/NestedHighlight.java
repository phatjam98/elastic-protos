package com.phatjam98.elasticprotos.engine;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Highlighted fragments of one nested object of a hit, such as one party of a document. The
 * compiler asks for them through {@code inner_hits} on the {@code nested} query clauses of the
 * search, and the adapters read them back into {@link Hit#nestedHighlights()}.
 *
 * <p>{@code offsets} and {@code highlight} are copied on construction and returned unmodifiable.
 *
 * @param path the dotted nested path of the object, such as {@code parties} or {@code
 *     shelves.books}
 * @param offsets the position of the object in its array at each nested level, outermost first:
 *     {@code [1]} for {@code parties[1]}, {@code [0, 2]} for the third book of the first shelf
 * @param highlight highlighted fragments per field, by the field's full dotted path such as
 *     {@code parties.name}
 */
public record NestedHighlight(String path, List<Integer> offsets,
    Map<String, List<String>> highlight) {

  /** Copies the offsets and the fragments. */
  public NestedHighlight {
    Objects.requireNonNull(path, "path");
    offsets = List.copyOf(offsets);
    highlight = Copies.stringLists(highlight);
  }
}
