package com.phatjam98.elasticprotos.mapping;

import com.google.protobuf.Descriptors.Descriptor;
import java.util.ArrayList;
import java.util.Arrays;
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
 *     counters.*}, {@code children.*.count} or, for a typed map inside the values of another,
 *     {@code outer.*.inner.*}; see {@link #resolve(String)}
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
   * <p>A path the mapping declares resolves to its field. Otherwise the path is matched segment by
   * segment against the patterns of {@link #dynamicFields()}: a {@code *} stands for exactly one
   * key, which must not be empty or hold {@code *}, and every other segment must be equal. {@code
   * counters.views} resolves to {@code counters.*}, {@code children.k.count} to {@code
   * children.*.count}, and, for a typed map inside the values of another, {@code outer.a.inner.b}
   * to {@code outer.*.inner.*}. Keys never contain dots, so at most one pattern matches. The
   * returned field carries the concrete path, the proto path with every {@code *} replaced by its
   * key, and {@link FieldInfo#pattern()}.
   *
   * @param path a dotted index path
   * @return the field, or empty when the path is not mapped
   */
  public Optional<FieldInfo> resolve(String path) {
    FieldInfo declared = fields.get(path);
    if (declared != null) {
      return Optional.of(declared);
    }
    if (dynamicFields.isEmpty()) {
      return Optional.empty();
    }
    String[] segments = path.split("\\.", -1);
    for (FieldInfo pattern : dynamicFields.values()) {
      String[] patternSegments = pattern.path().split("\\.", -1);
      if (matches(patternSegments, segments, segments.length)) {
        return Optional.of(instantiate(pattern, patternSegments, segments, path));
      }
    }
    return Optional.empty();
  }

  /**
   * Returns the values of the typed map a path runs through: the {@code P.*} pattern field of the
   * map {@code P} whose key is a segment of the path; the innermost one when typed maps are nested
   * in the values of others.
   *
   * @param path a dotted index path such as {@code children.k.count}
   * @return the {@code P.*} field, or empty when the path is not below the key of a typed map
   */
  public Optional<FieldInfo> typedMapValues(String path) {
    if (dynamicFields.isEmpty()) {
      return Optional.empty();
    }
    String[] segments = path.split("\\.", -1);
    FieldInfo innermost = null;
    int depth = 0;
    for (FieldInfo info : dynamicFields.values()) {
      String[] patternSegments = info.path().split("\\.", -1);
      if (info.path().endsWith(".*") && patternSegments.length > depth
          && matches(patternSegments, segments, patternSegments.length)) {
        innermost = info;
        depth = patternSegments.length;
      }
    }
    return Optional.ofNullable(innermost);
  }

  /**
   * Returns the nested paths that enclose a field path, outermost first. Nested fields inside the
   * values of typed maps are instantiated with the path's keys: with {@code children.*.items}
   * nested, {@code children.k.items.sku} is enclosed by {@code children.k.items}, and with {@code
   * outer.*.inner} and {@code outer.*.inner.*} nested, {@code outer.a.inner.b.n} by {@code
   * outer.a.inner} and {@code outer.a.inner.b}. A pattern path (one whose key is {@code *}) has no
   * enclosing nested paths inside typed map values.
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
    if (!dynamicFields.isEmpty()) {
      String[] segments = path.split("\\.", -1);
      for (FieldInfo info : dynamicFields.values()) {
        String[] patternSegments = info.path().split("\\.", -1);
        if (info.kind() == FieldKind.NESTED && patternSegments.length < segments.length
            && matches(patternSegments, segments, patternSegments.length)) {
          ancestors.add(String.join(".", Arrays.copyOf(segments, patternSegments.length)));
        }
      }
    }
    ancestors.sort(null);
    return List.copyOf(ancestors);
  }

  /**
   * Returns whether the first {@code length} segments of a path match a pattern of exactly that
   * many segments. A {@code *} segment matches one key: not empty and without {@code *}.
   */
  private static boolean matches(String[] pattern, String[] path, int length) {
    if (pattern.length != length || path.length < length) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if ("*".equals(pattern[i])) {
        if (path[i].isEmpty() || path[i].indexOf('*') >= 0) {
          return false;
        }
      } else if (!pattern[i].equals(path[i])) {
        return false;
      }
    }
    return true;
  }

  /** Instantiates a pattern field for a concrete path: every {@code *} takes its key. */
  private static FieldInfo instantiate(FieldInfo pattern, String[] patternSegments,
      String[] segments, String path) {
    String[] protoSegments = pattern.protoPath().split("\\.", -1);
    for (int i = 0; i < patternSegments.length && i < protoSegments.length; i++) {
      if ("*".equals(patternSegments[i])) {
        protoSegments[i] = segments[i];
      }
    }
    return new FieldInfo(path, String.join(".", protoSegments), pattern.kind(), pattern.repeated(),
        pattern.keywordSubfield(), pattern.boost(), pattern.field(), pattern.path(),
        pattern.enabled());
  }
}
