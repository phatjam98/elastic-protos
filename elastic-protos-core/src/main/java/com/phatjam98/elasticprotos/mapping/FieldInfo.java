package com.phatjam98.elasticprotos.mapping;

import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * What the mapper decided about one field: where it lives in the index and how it is typed.
 *
 * @param path dotted path of the field in the index (after any name overrides)
 * @param protoPath dotted path of the field in the proto (before any name overrides)
 * @param kind the mapped type
 * @param repeated whether the proto field is repeated or a map
 * @param keywordSubfield whether a text field carries a {@code .keyword} sub-field
 * @param boost default boost when the field is searched by {@code FULL_TEXT} without a field list
 * @param field the proto field this was derived from
 */
public record FieldInfo(
    String path,
    String protoPath,
    FieldKind kind,
    boolean repeated,
    boolean keywordSubfield,
    float boost,
    FieldDescriptor field) {

  /** Returns the last segment of {@link #path()}: the field's own name in the index. */
  public String esName() {
    return path.substring(path.lastIndexOf('.') + 1);
  }

  /** Returns the path to use for exact matching and sorting: the keyword sub-field for text. */
  public String exactMatchPath() {
    return kind.isText() && keywordSubfield ? path + ".keyword" : path;
  }

  /** Returns whether exact matching is possible on this field. */
  public boolean supportsExactMatch() {
    return !kind.isText() || keywordSubfield;
  }
}
