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
 * the {@code properties} of an object or nested field with {@code dynamic: true} in the desired
 * mappings, and of a {@code flattened} or {@code flat_object} field, are ignored, so sub-fields
 * picked up from indexed documents (for example the keys of a proto {@code map}) are never reported
 * as removed. When only the current mapping is dynamic, its picked-up sub-fields are still never
 * reported as removed, but they are compared with the typed properties the desired mapping
 * declares, so a picked-up key whose type differs is a type change.
 *
 * <p>{@code dynamic} is compared by its effective value: a node that does not declare it inherits
 * it from its parent, and the root defaults to {@code true}. A change is reported on the root and
 * on every node that declares {@code dynamic} on either side, with the effective values as {@code
 * before} and {@code after}.
 *
 * <p>An addition that existing documents were never indexed into is also listed in {@link
 * MappingDiff#unindexedAdditions()}: a multi-field added under a field that already exists (for
 * example {@code name.keyword} under {@code name}), and any property added under a parent (the
 * root included) whose current effective {@code dynamic} is {@code false} or {@code runtime}, where
 * values of unmapped fields sit in {@code _source} without being indexed. Under {@code dynamic:
 * true} the engine maps every key it sees, so a property missing from the current mapping is in no
 * old document and is a plain addition; under {@code strict} no old document can have it.
 *
 * <p>Settings are compared only for keys the desired settings declare, by string value, against
 * the engine's {@code settings.index} object. Shard and replica counts cannot be migrated, so
 * differences there never set {@link MappingDiff#settingsChanged()}; see {@link
 * #settingsWarnings}.
 */
public final class DiffClassifier {

  /** Path used for root-level parameter changes. */
  public static final String ROOT = "<root>";

  /**
   * The only mapping parameters applied in place. A change to any other parameter ({@code
   * analyzer}, {@code search_analyzer}, {@code index}, {@code doc_values}, {@code format}, {@code
   * ignore_above}, {@code normalizer}, {@code copy_to}, {@code null_value}, {@code coerce},
   * {@code enabled}, and any parameter not listed here) needs a reindex, because either the engine
   * refuses it or documents already indexed would not reflect it.
   */
  public static final Set<String> UPDATABLE_PARAMS = Set.of("dynamic");

  /** The dynamic mode of a mapping that does not declare one. */
  private static final String DEFAULT_DYNAMIC = "true";

  /** Dynamic modes under which values of unmapped fields are kept in _source but not indexed. */
  private static final Set<String> UNINDEXED_DYNAMIC = Set.of("false", "runtime");

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
    String beforeDynamic = dynamic(before, DEFAULT_DYNAMIC);
    String afterDynamic = dynamic(after, DEFAULT_DYNAMIC);
    if (!beforeDynamic.equals(afterDynamic)) {
      out.paramChanged.add(new Change(ROOT + ":dynamic", beforeDynamic, afterDynamic));
    }
    diffParams(ROOT, before, after, out);
    diffChildren("", map(before.get("properties")), map(after.get("properties")), beforeDynamic,
        afterDynamic, UNINDEXED_DYNAMIC.contains(beforeDynamic), true, out);
    return new MappingDiff(out.added, out.removed, out.typeChanged, out.paramChanged,
        settingsChanged(existingSettings, desiredSettings), out.unindexed);
  }

  /**
   * Classifies a diff.
   *
   * <p>Type changes, changes to any parameter not in {@link #UPDATABLE_PARAMS}, and {@link
   * MappingDiff#unindexedAdditions() additions old documents were never indexed into} need a
   * reindex; removed fields need one only under {@link RemovedFieldPolicy#REINDEX}; other added
   * fields and {@code dynamic} changes are applied in place; anything else is a no-op. Settings
   * differences alone are a no-op.
   *
   * @param diff the differences
   * @param removedFields how removed fields are treated
   * @return {@link Action#NO_OP}, {@link Action#PUT_MAPPING} or {@link Action#REINDEX}
   */
  public static Action classify(MappingDiff diff, RemovedFieldPolicy removedFields) {
    if (!diff.typeChanged().isEmpty() || !diff.unindexedAdditions().isEmpty()) {
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

  /**
   * Compares the children of one node.
   *
   * @param beforeDynamic the effective dynamic mode of the parent's current mapping
   * @param afterDynamic the effective dynamic mode of the parent's desired mapping
   * @param unindexed whether additions here are fields old documents were never indexed into
   * @param reportRemoved whether children only in {@code before} are reported as removed
   */
  private static void diffChildren(
      String prefix, Map<String, Object> before, Map<String, Object> after, String beforeDynamic,
      String afterDynamic, boolean unindexed, boolean reportRemoved, Collector out) {
    for (Map.Entry<String, Object> entry : after.entrySet()) {
      String path = prefix + entry.getKey();
      Object existing = before.get(entry.getKey());
      if (existing == null) {
        Change change = new Change(path, null, entry.getValue());
        out.added.add(change);
        if (unindexed) {
          out.unindexed.add(change);
        }
      } else {
        diffNode(path, map(existing), map(entry.getValue()), beforeDynamic, afterDynamic, out);
      }
    }
    if (!reportRemoved) {
      return;
    }
    for (Map.Entry<String, Object> entry : before.entrySet()) {
      if (!after.containsKey(entry.getKey())) {
        out.removed.add(new Change(prefix + entry.getKey(), entry.getValue(), null));
      }
    }
  }

  private static void diffNode(String path, Map<String, Object> before,
      Map<String, Object> after, String inheritedBefore, String inheritedAfter, Collector out) {
    Object beforeType = before.get("type");
    Object afterType = after.get("type");
    if (!Objects.equals(beforeType, afterType)) {
      out.typeChanged.add(new Change(path, beforeType, afterType));
      return;
    }
    String beforeDynamic = dynamic(before, inheritedBefore);
    String afterDynamic = dynamic(after, inheritedAfter);
    if ((before.containsKey("dynamic") || after.containsKey("dynamic"))
        && !beforeDynamic.equals(afterDynamic)) {
      out.paramChanged.add(new Change(path + ":dynamic", beforeDynamic, afterDynamic));
    }
    diffParams(path, before, after, out);
    if (!isOpenEnded(after)) {
      Map<String, Object> beforeProperties = map(before.get("properties"));
      Map<String, Object> afterProperties = map(after.get("properties"));
      // Under a dynamic: true current mapping the keys the engine picked up are compared with
      // the typed properties (a differing type is a type change) but are never removals.
      boolean pickedUp = isOpenEnded(before);
      diffChildren(path + ".", beforeProperties, afterProperties, beforeDynamic, afterDynamic,
          !pickedUp && UNINDEXED_DYNAMIC.contains(beforeDynamic), !pickedUp, out);
    }
    // A multi-field added to an existing field is empty for every document indexed before it.
    diffChildren(path + ".", map(before.get("fields")), map(after.get("fields")), beforeDynamic,
        afterDynamic, true, true, out);
  }

  /** Returns a normalized node's declared dynamic mode, or the inherited one. */
  private static String dynamic(Map<String, Object> node, String inherited) {
    Object value = node.get("dynamic");
    return value == null ? inherited : value.toString();
  }

  private static boolean isContainer(Map<String, Object> node) {
    Object type = node.get("type");
    return "object".equals(type) || "nested".equals(type);
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
    return isContainer(node) && DEFAULT_DYNAMIC.equals(node.get("dynamic"));
  }

  private static void diffParams(
      String path, Map<String, Object> before, Map<String, Object> after, Collector out) {
    Set<String> keys = new TreeSet<>(before.keySet());
    keys.addAll(after.keySet());
    keys.removeAll(STRUCTURAL);
    keys.remove("dynamic");
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
    private final List<Change> unindexed = new ArrayList<>();
  }
}
