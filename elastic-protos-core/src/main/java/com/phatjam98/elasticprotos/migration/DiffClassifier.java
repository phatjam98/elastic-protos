package com.phatjam98.elasticprotos.migration;

import com.phatjam98.elasticprotos.migration.MappingDiff.Change;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Compares current and desired mappings and decides whether a change can be applied in place.
 *
 * <p>Both trees are normalized before comparing: the root {@code _meta} is dropped, an object with
 * {@code properties} and no {@code type} is {@code type: object}, scalars (including
 * {@code dynamic}) are compared by their string form, a single {@code copy_to} string is a
 * one-element list, and key order is ignored.
 *
 * <p>Fields whose children the engine adds on its own are compared by their own parameters only:
 * the {@code properties} of an object or nested field with {@code dynamic: true} on either side,
 * and of a {@code flattened} or {@code flat_object} field, are ignored, so sub-fields picked up
 * from indexed documents (for example the keys of a proto {@code map}) are never reported as
 * removed.
 *
 * <p>Settings are compared only for keys the desired settings declare, by string value, against
 * the engine's {@code settings.index} object. Shard and replica counts cannot be migrated, so
 * differences there never set {@link MappingDiff#settingsChanged()}; see {@link
 * #settingsWarnings}.
 */
public final class DiffClassifier {

  /** Path used for root-level parameter changes. */
  public static final String ROOT = "<root>";

  /** Mapping parameters that cannot change without a reindex. */
  public static final Set<String> REINDEX_PARAMS = Set.of(
      "analyzer", "search_analyzer", "index", "doc_values", "format", "ignore_above",
      "normalizer", "copy_to", "null_value", "coerce");

  /** Parameters that the engine can update in place. */
  public static final Set<String> UPDATABLE_PARAMS = Set.of("dynamic");

  private static final Set<String> STRUCTURAL = Set.of("type", "properties", "fields");
  private static final Set<String> FIXED_SETTINGS =
      Set.of("number_of_shards", "number_of_replicas");

  private DiffClassifier() {}

  /**
   * Computes the differences.
   *
   * @param existingMappings the index's mappings as the engine returns them
   * @param desiredMappings the mappings the proto produces
   * @param existingSettings the index's settings as the engine returns them, or empty
   * @param desiredSettings the settings the proto produces, or empty
   * @return the differences
   */
  public static MappingDiff diff(
      Map<String, Object> existingMappings, Map<String, Object> desiredMappings,
      Map<String, Object> existingSettings, Map<String, Object> desiredSettings) {
    Map<String, Object> before = normalizeNode(existingMappings, true);
    Map<String, Object> after = normalizeNode(desiredMappings, true);
    Collector out = new Collector();
    diffParams(ROOT, before, after, out);
    diffChildren("", map(before.get("properties")), map(after.get("properties")), out);
    return new MappingDiff(out.added, out.removed, out.typeChanged, out.paramChanged,
        settingsChanged(existingSettings, desiredSettings));
  }

  /**
   * Classifies a diff.
   *
   * <p>Type changes and changes to any parameter other than {@code dynamic} need a reindex;
   * removed fields need one only under {@link RemovedFieldPolicy#REINDEX}; added fields and
   * {@code dynamic} changes are applied in place; anything else is a no-op. Settings differences
   * alone are a no-op.
   *
   * @param diff the differences
   * @param removedFields how removed fields are treated
   * @return {@link Action#NO_OP}, {@link Action#PUT_MAPPING} or {@link Action#REINDEX}
   */
  public static Action classify(MappingDiff diff, RemovedFieldPolicy removedFields) {
    if (!diff.typeChanged().isEmpty()) {
      return Action.REINDEX;
    }
    for (Change change : diff.paramChanged()) {
      if (!UPDATABLE_PARAMS.contains(param(change.path()))) {
        return Action.REINDEX;
      }
    }
    if (!diff.removed().isEmpty() && removedFields == RemovedFieldPolicy.REINDEX) {
      return Action.REINDEX;
    }
    if (!diff.added().isEmpty() || !diff.paramChanged().isEmpty()) {
      return Action.PUT_MAPPING;
    }
    return Action.NO_OP;
  }

  /**
   * Describes shard and replica count differences, which are never applied.
   *
   * @param existingSettings the index's settings as the engine returns them
   * @param desiredSettings the settings the proto produces
   * @return one warning per differing setting
   */
  public static List<String> settingsWarnings(
      Map<String, Object> existingSettings, Map<String, Object> desiredSettings) {
    Map<String, Object> existing = flatten(existingSettings);
    Map<String, Object> desired = flatten(desiredSettings);
    List<String> warnings = new ArrayList<>();
    for (String key : new TreeSet<>(FIXED_SETTINGS)) {
      if (desired.containsKey(key)
          && !Objects.equals(normalizeValue(desired.get(key)), normalizeValue(existing.get(key)))) {
        warnings.add(key + " is " + existing.get(key) + " but the proto asks for "
            + desired.get(key) + "; not applied");
      }
    }
    return warnings;
  }

  /**
   * Returns the parameter name of a parameter change path, or the path itself.
   *
   * @param path a {@link Change#path()}
   * @return the part after the last {@code :}
   */
  public static String param(String path) {
    int colon = path.lastIndexOf(':');
    return colon < 0 ? path : path.substring(colon + 1);
  }

  /**
   * Normalizes a mappings tree for comparison.
   *
   * @param mappings a mappings object
   * @return the normalized copy
   */
  public static Map<String, Object> normalize(Map<String, Object> mappings) {
    return normalizeNode(mappings, true);
  }

  // ------------------------------------------------------------------ mappings

  private static void diffChildren(
      String prefix, Map<String, Object> before, Map<String, Object> after, Collector out) {
    for (Map.Entry<String, Object> entry : after.entrySet()) {
      String path = prefix + entry.getKey();
      Object existing = before.get(entry.getKey());
      if (existing == null) {
        out.added.add(new Change(path, null, entry.getValue()));
      } else {
        diffNode(path, map(existing), map(entry.getValue()), out);
      }
    }
    for (Map.Entry<String, Object> entry : before.entrySet()) {
      if (!after.containsKey(entry.getKey())) {
        out.removed.add(new Change(prefix + entry.getKey(), entry.getValue(), null));
      }
    }
  }

  private static void diffNode(
      String path, Map<String, Object> before, Map<String, Object> after, Collector out) {
    Object beforeType = before.get("type");
    Object afterType = after.get("type");
    if (!Objects.equals(beforeType, afterType)) {
      out.typeChanged.add(new Change(path, beforeType, afterType));
      return;
    }
    diffParams(path, before, after, out);
    if (!isOpenEnded(before) && !isOpenEnded(after)) {
      diffChildren(path + ".", map(before.get("properties")), map(after.get("properties")), out);
    }
    diffChildren(path + ".", map(before.get("fields")), map(after.get("fields")), out);
  }

  /**
   * Returns whether a normalized node's sub-properties are owned by the engine rather than the
   * proto: an object or nested field with {@code dynamic: true}, or a {@code flattened} /
   * {@code flat_object} field. Their children appear as documents are indexed, so they are never
   * compared.
   */
  private static boolean isOpenEnded(Map<String, Object> node) {
    Object type = node.get("type");
    if ("flattened".equals(type) || "flat_object".equals(type)) {
      return true;
    }
    return ("object".equals(type) || "nested".equals(type))
        && "true".equals(node.get("dynamic"));
  }

  private static void diffParams(
      String path, Map<String, Object> before, Map<String, Object> after, Collector out) {
    Set<String> keys = new TreeSet<>(before.keySet());
    keys.addAll(after.keySet());
    keys.removeAll(STRUCTURAL);
    for (String key : keys) {
      Object beforeValue = before.get(key);
      Object afterValue = after.get(key);
      if (!Objects.equals(beforeValue, afterValue)) {
        out.paramChanged.add(new Change(path + ":" + key, beforeValue, afterValue));
      }
    }
  }

  private static Map<String, Object> normalizeNode(Map<String, Object> node, boolean root) {
    Map<String, Object> copy = new TreeMap<>();
    for (Map.Entry<String, Object> entry : node.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      if (root && "_meta".equals(key)) {
        continue;
      }
      if (("properties".equals(key) || "fields".equals(key)) && value instanceof Map) {
        Map<String, Object> children = new TreeMap<>();
        map(value).forEach((name, child) ->
            children.put(name, normalizeNode(map(child), false)));
        copy.put(key, children);
      } else if ("copy_to".equals(key) && value instanceof String) {
        copy.put(key, List.of(value));
      } else {
        copy.put(key, normalizeValue(value));
      }
    }
    if (!root && !copy.containsKey("type")) {
      copy.put("type", "object");
    }
    return copy;
  }

  private static Object normalizeValue(Object value) {
    if (value instanceof Map) {
      Map<String, Object> copy = new TreeMap<>();
      map(value).forEach((key, item) -> copy.put(key, normalizeValue(item)));
      return copy;
    }
    if (value instanceof List) {
      List<Object> copy = new ArrayList<>();
      for (Object item : (List<?>) value) {
        copy.add(normalizeValue(item));
      }
      return copy;
    }
    return value == null ? null : value.toString();
  }

  // ------------------------------------------------------------------ settings

  private static boolean settingsChanged(
      Map<String, Object> existingSettings, Map<String, Object> desiredSettings) {
    Map<String, Object> existing = flatten(existingSettings);
    for (Map.Entry<String, Object> entry : flatten(desiredSettings).entrySet()) {
      if (FIXED_SETTINGS.contains(entry.getKey())) {
        continue;
      }
      if (!Objects.equals(normalizeValue(entry.getValue()),
          normalizeValue(existing.get(entry.getKey())))) {
        return true;
      }
    }
    return false;
  }

  private static Map<String, Object> flatten(Map<String, Object> settings) {
    Map<String, Object> flat = new TreeMap<>();
    if (settings == null) {
      return flat;
    }
    for (Map.Entry<String, Object> entry : settings.entrySet()) {
      String key = entry.getKey();
      if ("index".equals(key) && entry.getValue() instanceof Map) {
        flat.putAll(flatten(map(entry.getValue())));
      } else {
        flat.put(key.startsWith("index.") ? key.substring("index.".length()) : key,
            entry.getValue());
      }
    }
    return flat;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : Map.of();
  }

  /** Accumulates changes while walking the trees. */
  private static final class Collector {
    private final List<Change> added = new ArrayList<>();
    private final List<Change> removed = new ArrayList<>();
    private final List<Change> typeChanged = new ArrayList<>();
    private final List<Change> paramChanged = new ArrayList<>();
  }
}
