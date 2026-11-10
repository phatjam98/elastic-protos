package com.phatjam98.elasticprotos.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Highlighted fragments of one nested object of a hit, such as one party of a document. The
 * compiler asks for them through {@code inner_hits} on the {@code nested} query clauses of the
 * search, and the adapters read them back into {@link Hit#nestedHighlights()}.
 *
 * <p>{@code levels} locate the object: one {@link NestedLevel} per nested level, outermost first.
 * To find the object in the message read back with {@code ProtoDocumentCodec.fromHit}, follow the
 * levels from the root message: each level's field is the index path of a repeated message field
 * relative to the previous level (dots step through singular message fields), and its offset is
 * the position in that repeated field, since the codec writes repeated messages in order. For
 * {@code [sections@1, meta.annotations@0]} that is {@code
 * binder.getSections(1).getMeta().getAnnotations(0)}. Index
 * paths equal the proto field names unless a field option renames them; {@code
 * IndexDefinition.field(path).protoPath()} gives the proto path of an index path.
 *
 * <p>{@code levels} and {@code highlight} are copied on construction and returned unmodifiable;
 * null becomes empty.
 *
 * @param levels the position of the object, outermost level first
 * @param highlight highlighted fragments per field, by the field's full dotted index path such as
 *     {@code parties.name}
 */
public record NestedHighlight(List<NestedLevel> levels, Map<String, List<String>> highlight) {

  /** Copies the levels and the fragments. */
  public NestedHighlight {
    levels = levels == null ? List.of() : List.copyOf(levels);
    highlight = highlight == null ? Map.of() : Copies.stringLists(highlight);
  }

  /**
   * Returns the full dotted index path of the nested object, such as {@code parties} or {@code
   * boxes.meta.notes}; use {@link #levels()} to tell which segments each offset belongs to.
   */
  public String path() {
    List<String> fields = new ArrayList<>();
    for (NestedLevel level : levels) {
      fields.add(level.field());
    }
    return String.join(".", fields);
  }

  /** Returns the offset of each level, outermost first. */
  public List<Integer> offsets() {
    List<Integer> offsets = new ArrayList<>();
    for (NestedLevel level : levels) {
      offsets.add(level.offset());
    }
    return Collections.unmodifiableList(offsets);
  }
}
