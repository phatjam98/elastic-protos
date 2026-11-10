package com.phatjam98.elasticprotos.mapping;

import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * What the mapper decided about one field: where it lives in the index and how it is typed.
 *
 * <p>Fields below the keys of a typed map (a {@code map} mapped as {@code OBJECT} or {@code
 * NESTED}) are dynamic: the mapper describes them by a pattern such as {@code counters.*} or
 * {@code children.*.count}, and {@link IndexDefinition#resolve(String)} instantiates the pattern
 * for a concrete key.
 *
 * @param path dotted path of the field in the index (after any name overrides); for a dynamic
 *     field either its pattern or a concrete path such as {@code counters.views}
 * @param protoPath dotted path of the field in the proto (before any name overrides); {@code *}
 *     stands for the map key in a pattern
 * @param kind the mapped type
 * @param repeated whether the proto field is repeated or a map
 * @param keywordSubfield whether a text field carries a {@code .keyword} sub-field
 * @param boost default boost when the field is searched by {@code FULL_TEXT} without a field list
 * @param field the proto field this was derived from; the map entry's {@code value} field for the
 *     values of a typed map
 * @param pattern the index path pattern of a dynamic field, such as {@code counters.*}; null for a
 *     field the mapping declares
 * @param enabled false for an object mapped with {@code enabled: false}: the engine keeps it in
 *     {@code _source} without parsing it, so it cannot be searched, not even with {@code EXISTS}
 */
public record FieldInfo(
    String path,
    String protoPath,
    FieldKind kind,
    boolean repeated,
    boolean keywordSubfield,
    float boost,
    FieldDescriptor field,
    String pattern,
    boolean enabled) {

  /** Creates the description of an enabled field, dynamic when {@code pattern} is not null. */
  public FieldInfo(
      String path,
      String protoPath,
      FieldKind kind,
      boolean repeated,
      boolean keywordSubfield,
      float boost,
      FieldDescriptor field,
      String pattern) {
    this(path, protoPath, kind, repeated, keywordSubfield, boost, field, pattern, true);
  }

  /** Creates the description of a field the mapping declares (not dynamic). */
  public FieldInfo(
      String path,
      String protoPath,
      FieldKind kind,
      boolean repeated,
      boolean keywordSubfield,
      float boost,
      FieldDescriptor field) {
    this(path, protoPath, kind, repeated, keywordSubfield, boost, field, null);
  }

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

  /**
   * Returns whether the field lives below the keys of a typed map, so that the engine maps it
   * through a dynamic template when a document first uses the key.
   */
  public boolean isDynamic() {
    return pattern != null;
  }
}
