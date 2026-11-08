package com.phatjam98.elasticprotos.mapping;

import com.google.protobuf.Descriptors.Descriptor;
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
    List<String> warnings) {

  /** Defensive, sorted and unmodifiable copies of every collection. */
  public IndexDefinition {
    settings = Collections.unmodifiableMap(new TreeMap<>(settings));
    mappings = Collections.unmodifiableMap(new TreeMap<>(mappings));
    fields = Collections.unmodifiableMap(new TreeMap<>(fields));
    nestedPaths = Collections.unmodifiableSet(new TreeSet<>(nestedPaths));
    fullTextFields = Collections.unmodifiableMap(new TreeMap<>(fullTextFields));
    ignoredPaths = Collections.unmodifiableSet(new TreeSet<>(ignoredPaths));
    warnings = List.copyOf(warnings);
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
   * Returns the nested paths that enclose a field path, outermost first.
   *
   * @param path a dotted index path
   * @return the enclosing nested paths, possibly empty
   */
  public List<String> nestedAncestors(String path) {
    return nestedPaths.stream()
        .filter(nested -> path.startsWith(nested + "."))
        .sorted()
        .toList();
  }
}
