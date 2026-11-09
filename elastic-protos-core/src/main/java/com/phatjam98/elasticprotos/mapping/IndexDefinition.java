package com.phatjam98.elasticprotos.mapping;

import com.google.protobuf.Descriptors.Descriptor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The engine-agnostic result of mapping a message: the index to create plus everything the codec,
 * query compiler and migrator need to know about its fields.
 *
 * @param descriptor the root message
 * @param alias the alias applications read and write through
 * @param name the versioned concrete index name
 * @param contentHash short hash of the mappings; changes whenever the mappings change
 * @param settings index settings (shards, replicas, analysis), possibly empty
 * @param mappings the {@code mappings} object including {@code _meta}
 * @param fields every mapped field by index path
 * @param nestedPaths paths mapped as {@code nested}
 * @param fullTextFields paths searched by {@code FULL_TEXT} by default, with their boosts
 * @param ignoredPaths proto fields excluded from the mapping and from documents
 * @param idField path of the field whose value is the document id, or null
 * @param warnings non-fatal problems found while mapping
 * @param dynamicFields the fields below the keys of typed maps by pattern path, such as {@code
 *     counters.*} or {@code children.*.count}; see {@link #resolve(String)}
 */
public record IndexDefinition(
    Descriptor descriptor,
    String alias,
    String name,
    String contentHash,
    Map<String, Object> settings,
    Map<String, Object> mappings,
    Map<String, FieldInfo> fields,
    Set<String> nestedPaths,
    Map<String, Float> fullTextFields,
    Set<String> ignoredPaths,
    String idField,
    List<String> warnings,
    Map<String, FieldInfo> dynamicFields) {

  /** Defensive, sorted and unmodifiable copies of every collection. */
  public IndexDefinition {
    settings = Collections.unmodifiableMap(new TreeMap<>(settings));
    mappings = Collections.unmodifiableMap(new TreeMap<>(mappings));
    fields = Collections.unmodifiableMap(new TreeMap<>(fields));
    nestedPaths = Collections.unmodifiableSet(new TreeSet<>(nestedPaths));
    fullTextFields = Collections.unmodifiableMap(new TreeMap<>(fullTextFields));
    ignoredPaths = Collections.unmodifiableSet(new TreeSet<>(ignoredPaths));
    warnings = List.copyOf(warnings);
    dynamicFields = Collections.unmodifiableMap(
        new TreeMap<>(dynamicFields == null ? Map.of() : dynamicFields));
  }

  /**
   * Creates a definition without typed maps.
   *
   * @param descriptor the root message
   * @param alias the alias applications read and write through
   * @param name the versioned concrete index name
   * @param contentHash short hash of the mappings
   * @param settings index settings
   * @param mappings the {@code mappings} object including {@code _meta}
   * @param fields every mapped field by index path
   * @param nestedPaths paths mapped as {@code nested}
   * @param fullTextFields paths searched by {@code FULL_TEXT} by default, with their boosts
   * @param ignoredPaths proto fields excluded from the mapping and from documents
   * @param idField path of the id field, or null
   * @param warnings non-fatal problems found while mapping
   */
  public IndexDefinition(
      Descriptor descriptor,
      String alias,
      String name,
      String contentHash,
      Map<String, Object> settings,
      Map<String, Object> mappings,
      Map<String, FieldInfo> fields,
      Set<String> nestedPaths,
      Map<String, Float> fullTextFields,
      Set<String> ignoredPaths,
      String idField,
      List<String> warnings) {
    this(descriptor, alias, name, contentHash, settings, mappings, fields, nestedPaths,
        fullTextFields, ignoredPaths, idField, warnings, Map.of());
  }

  /** Returns the proto full name of the root message. */
  public String protoFullName() {
    return descriptor.getFullName();
  }

  /** Looks up a field by its index path. */
  public Optional<FieldInfo> field(String path) {
    return Optional.ofNullable(fields.get(path));
  }

  /**
   * Resolves a field path, including paths below the keys of typed maps.
   *
   * <p>A path the mapping declares resolves to its field. Otherwise, when the path runs through a
   * typed map {@code P} (a {@code map} mapped as {@code OBJECT} or {@code NESTED}), the segment
   * after {@code P} is the key and the rest {@code R} resolves to the pattern {@code P.*} (no
   * rest) or {@code P.*.R}: {@code counters.views} resolves to {@code counters.*}, and {@code
   * children.k.count} to {@code children.*.count}. The returned field carries the concrete path
   * and {@link FieldInfo#pattern()}.
   *
   * @param path a dotted index path
   * @return the field, or empty when the path is not mapped
   */
  public Optional<FieldInfo> resolve(String path) {
    FieldInfo declared = fields.get(path);
    if (declared != null) {
      return Optional.of(declared);
    }
    MapKey key = mapKey(path);
    if (key == null) {
      return Optional.empty();
    }
    FieldInfo pattern = dynamicFields.get(
        key.remainder().isEmpty() ? key.map() + ".*" : key.map() + ".*." + key.remainder());
    if (pattern == null) {
      return Optional.empty();
    }
    String protoPath = pattern.protoPath();
    int star = protoPath.indexOf(".*");
    String concreteProtoPath = star < 0 ? protoPath
        : protoPath.substring(0, star + 1) + key.key() + protoPath.substring(star + 2);
    return Optional.of(new FieldInfo(path, concreteProtoPath, pattern.kind(), pattern.repeated(),
        pattern.keywordSubfield(), pattern.boost(), pattern.field(), pattern.path()));
  }

  /**
   * Returns the values of the typed map a path runs through: the {@code P.*} pattern field of the
   * map {@code P} whose key is the segment after {@code P}.
   *
   * @param path a dotted index path such as {@code children.k.count}
   * @return the {@code P.*} field, or empty when the path is not below the key of a typed map
   */
  public Optional<FieldInfo> typedMapValues(String path) {
    MapKey key = mapKey(path);
    return key == null ? Optional.empty() : Optional.of(dynamicFields.get(key.map() + ".*"));
  }

  /**
   * Returns the nested paths that enclose a field path, outermost first. Nested fields inside the
   * values of a typed map are instantiated with the path's key: with {@code children.*.items}
   * nested, {@code children.k.items.sku} is enclosed by {@code children.k.items}.
   *
   * @param path a dotted index path
   * @return the enclosing nested paths, possibly empty
   */
  public List<String> nestedAncestors(String path) {
    List<String> ancestors = new ArrayList<>();
    for (String nested : nestedPaths) {
      if (path.startsWith(nested + ".")) {
        ancestors.add(nested);
      }
    }
    MapKey key = mapKey(path);
    if (key != null) {
      String patternPrefix = key.map() + ".*";
      for (FieldInfo info : dynamicFields.values()) {
        if (info.kind() == FieldKind.NESTED && info.path().startsWith(patternPrefix)) {
          String concrete = key.map() + "." + key.key()
              + info.path().substring(patternPrefix.length());
          if (path.startsWith(concrete + ".")) {
            ancestors.add(concrete);
          }
        }
      }
    }
    ancestors.sort(null);
    return List.copyOf(ancestors);
  }

  /**
   * Splits a path below the key of a typed map into the map path, the key and the rest.
   *
   * @return the parts, or null when the path is not below a typed map or the key is empty
   */
  private MapKey mapKey(String path) {
    if (dynamicFields.isEmpty()) {
      return null;
    }
    for (int dot = path.lastIndexOf('.'); dot > 0; dot = path.lastIndexOf('.', dot - 1)) {
      String map = path.substring(0, dot);
      if (!dynamicFields.containsKey(map + ".*")) {
        continue;
      }
      String rest = path.substring(dot + 1);
      int next = rest.indexOf('.');
      String key = next < 0 ? rest : rest.substring(0, next);
      String remainder = next < 0 ? "" : rest.substring(next + 1);
      if (key.isEmpty() || (next >= 0 && remainder.isEmpty())) {
        return null;
      }
      return new MapKey(map, key, remainder);
    }
    return null;
  }

  /** A path below the key of a typed map. */
  private record MapKey(String map, String key, String remainder) {}
}
