package com.phatjam98.elasticprotos.query;

import com.phatjam98.elasticprotos.mapping.FieldInfo;
import com.phatjam98.elasticprotos.mapping.FieldKind;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;

/**
 * Resolves paths below a {@code flattened} field, such as {@code labels.env} under a {@code
 * map<string, string> labels}. The engine indexes every key of a flattened field as a keyword
 * reachable by its dotted path.
 */
final class FlattenedKeys {

  private FlattenedKeys() {}

  /**
   * Returns the flattened field that encloses a path.
   *
   * @param index the index
   * @param name a dotted path
   * @return the enclosing flattened field, or null when there is none or the path ends with a dot
   */
  static FieldInfo root(IndexDefinition index, String name) {
    for (int dot = name.lastIndexOf('.'); dot > 0; dot = name.lastIndexOf('.', dot - 1)) {
      FieldInfo root = index.fields().get(name.substring(0, dot));
      if (root == null || root.kind() != FieldKind.FLATTENED) {
        continue;
      }
      return dot == name.length() - 1 ? null : root;
    }
    return null;
  }

  /**
   * Describes a key of a flattened field.
   *
   * @param root the flattened field, from {@link #root(IndexDefinition, String)}
   * @param name the full dotted path of the key
   * @return a {@code FLATTENED} field description whose path is the key's path
   */
  static FieldInfo key(FieldInfo root, String name) {
    String key = name.substring(root.path().length() + 1);
    return new FieldInfo(name, root.protoPath() + "." + key, FieldKind.FLATTENED,
        root.repeated(), false, root.boost(), root.field());
  }
}
