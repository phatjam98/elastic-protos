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
 * reported as removed, but typed properties the desired mapping adds are reported as additions.
 *
 * <p>An addition that existing documents were never indexed into is also listed in {@link
 * MappingDiff#unindexedAdditions()}: a multi-field added under a field that already exists (for
 * example {@code name.keyword} under {@code name}), and a typed property added under an object or
 * nested field whose current mapping is dynamic ({@code dynamic: true}, or no properties at all
 * while dynamic mapping is enabled, inherited from its parents). Brand-new top-level fields and
 * properties added under an object that already has typed properties are plain additions.
 * Unindexed additions are {@link Action#BACKFILL backfilled} in place.
 *
 * <p>Settings are compared only for keys the desired settings declare, by string value, against
 * the engine's {@code settings.index} object, both flattened to dotted keys without the {@code
 * index.} prefix ({@code analysis.analyzer.folding.tokenizer} is one key). A differing key is
 * dynamic when it is in {@link #DYNAMIC_SETTINGS}, starts with one of {@link
 * #DYNAMIC_SETTING_PREFIXES} or is listed in {@link MigrationOptions#extraDynamicSettings()};
 * keys the engine or the migrator manages ({@link #MANAGED_SETTINGS}, {@code blocks.*}, {@code
 * version.*}) are ignored; everything else, including {@code number_of_shards}, {@code
 * analysis.*} and unknown keys, is static. {@code number_of_replicas} is not compared while the
 * desired {@code auto_expand_replicas} is set and not {@code false}. See {@link SettingsDiff}.
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

  private static final Set<String> STRUCTURAL = Set.of("type", "properties", "fields");

  /** Index settings the engines update on a live index, by key without {@code index.}. */
  public static final Set<String> DYNAMIC_SETTINGS = Set.of(
      "number_of_replicas", "auto_expand_replicas", "refresh_interval", "search.idle.after",
      "max_result_window", "max_inner_result_window", "max_rescore_window",
      "max_docvalue_fields_search", "max_script_fields", "max_ngram_diff", "max_shingle_diff",
      "max_terms_count", "max_regex_length", "max_refresh_listeners", "max_slices_per_scroll",
      "analyze.max_token_count", "highlight.max_analyzed_offset", "query.default_field",
      "default_pipeline", "final_pipeline", "gc_deletes", "hidden", "priority",
      "write.wait_for_active_shards", "unassigned.node_left.delayed_timeout",
      "routing.allocation.enable", "routing.rebalance.enable",
      "routing.allocation.total_shards_per_node", "mapping.total_fields.limit",
      "mapping.depth.limit", "mapping.nested_fields.limit", "mapping.nested_objects.limit",
      "mapping.field_name_length.limit", "translog.durability", "translog.flush_threshold_size");

  /** Prefixes of dynamic index settings. */
  public static final List<String> DYNAMIC_SETTING_PREFIXES = List.of(
      "routing.allocation.include.", "routing.allocation.exclude.",
      "routing.allocation.require.", "search.slowlog.", "indexing.slowlog.");

  /**
   * Index settings the engine or the migrator manages, never applied from the proto; {@code
   * blocks.*} (the migrator owns {@code blocks.write}) and {@code version.*} are managed too.
   */
  public static final Set<String> MANAGED_SETTINGS =
      Set.of("uuid", "creation_date", "provided_name");

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
    return diff(existingMappings, desiredMappings, existingSettings, desiredSettings, Set.of());
  }

  /**
   * Computes the differences, treating more setting keys as dynamic.
   *
   * @param existingMappings the index's mappings as the engine returns them
   * @param desiredMappings the mappings the proto produces
   * @param existingSettings the index's settings as the engine returns them, or empty
   * @param desiredSettings the settings the proto produces, or empty
   * @param extraDynamicSettings setting keys (without {@code index.}) to treat as dynamic
   * @return the differences
   */
  public static MappingDiff diff(
      Map<String, Object> existingMappings, Map<String, Object> desiredMappings,
      Map<String, Object> existingSettings, Map<String, Object> desiredSettings,
      Set<String> extraDynamicSettings) {
    Map<String, Object> before = normalizeNode(existingMappings, true);
    Map<String, Object> after = normalizeNode(desiredMappings, true);
    Collector out = new Collector();
    diffParams(ROOT, before, after, out);
    String dynamic = before.containsKey("dynamic")
        ? String.valueOf(before.get("dynamic")) : DEFAULT_DYNAMIC;
    diffChildren("", map(before.get("properties")), map(after.get("properties")), dynamic,
        false, true, out);
    SettingsDiff settings = settingsDiff(existingSettings, desiredSettings, extraDynamicSettings);
    return new MappingDiff(out.added, out.removed, out.typeChanged, out.paramChanged,
        !settings.isEmpty(), out.unindexed, settings);
  }

  /**
   * Classifies a diff.
   *
   * <p>Type changes and changes to any parameter not in {@link #UPDATABLE_PARAMS} need a reindex;
   * removed fields need one only under {@link RemovedFieldPolicy#REINDEX}. Otherwise, {@link
   * MappingDiff#unindexedAdditions() additions old documents were never indexed into} are
   * backfilled: the mapping is extended in place and the documents are re-indexed with an update
   * by query. Other added fields and {@code dynamic} changes are applied in place; anything else is
   * a no-op. Settings differences alone are a no-op.
   *
   * @param diff the differences
   * @param removedFields how removed fields are treated
   * @return {@link Action#NO_OP}, {@link Action#PUT_MAPPING}, {@link Action#BACKFILL} or {@link
   *     Action#REINDEX}
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
    if (!diff.unindexedAdditions().isEmpty()) {
      return Action.BACKFILL;
    }
    if (!diff.added().isEmpty() || !diff.paramChanged().isEmpty()) {
      return Action.PUT_MAPPING;
    }
    return Action.NO_OP;
  }

  /**
   * Describes the settings that a migration does not apply in place: static settings that differ,
   * and keys the engine or the migrator manages.
   *
   * @param existingSettings the index's settings as the engine returns them
   * @param desiredSettings the settings the proto produces
   * @return one warning per kind
   */
  public static List<String> settingsWarnings(
      Map<String, Object> existingSettings, Map<String, Object> desiredSettings) {
    SettingsDiff diff = settingsDiff(existingSettings, desiredSettings, Set.of());
    List<String> warnings = new ArrayList<>(managedWarnings(diff));
    if (!diff.staticChanges().isEmpty()) {
      warnings.add(staticWarning(diff));
    }
    return warnings;
  }

  /** Returns the warning for keys the engine or the migrator manages, or none. */
  static List<String> managedWarnings(SettingsDiff diff) {
    return diff.ignored().isEmpty() ? List.of() : List.of("settings managed by the engine or the"
        + " migrator are never applied: " + diff.ignored());
  }

  /** Returns the warning for static settings that differ. */
  static String staticWarning(SettingsDiff diff) {
    return "static settings differ; not applied: " + SettingsDiff.describe(diff.staticChanges());
  }

  /**
   * Compares the settings the proto declares with the index's settings.
   *
   * @param existingSettings the index's settings as the engine returns them, or null
   * @param desiredSettings the settings the proto produces, or null
   * @param extraDynamicSettings setting keys (without {@code index.}) to treat as dynamic
   * @return the differences
   */
  public static SettingsDiff settingsDiff(Map<String, Object> existingSettings,
      Map<String, Object> desiredSettings, Set<String> extraDynamicSettings) {
    Map<String, Object> existing = flattenSettings(existingSettings);
    Map<String, Object> desired = flattenSettings(desiredSettings);
    Object autoExpand = desired.get("auto_expand_replicas");
    boolean expands = autoExpand != null && !"false".equals(String.valueOf(autoExpand));
    List<Change> dynamicChanges = new ArrayList<>();
    List<Change> staticChanges = new ArrayList<>();
    List<String> ignored = new ArrayList<>();
    for (Map.Entry<String, Object> entry : desired.entrySet()) {
      String key = entry.getKey();
      if (isManagedSetting(key)) {
        ignored.add(key);
        continue;
      }
      if (expands && "number_of_replicas".equals(key)) {
        continue;
      }
      Object before = existing.get(key);
      if (Objects.equals(normalizeValue(entry.getValue()), normalizeValue(before))) {
        continue;
      }
      Change change = new Change(key, before, entry.getValue());
      if (isDynamicSetting(key, extraDynamicSettings)) {
        dynamicChanges.add(change);
      } else {
        staticChanges.add(change);
      }
    }
    return new SettingsDiff(dynamicChanges, staticChanges, ignored);
  }

  /** Returns whether a setting key (without {@code index.}) is managed and never applied. */
  public static boolean isManagedSetting(String key) {
    return MANAGED_SETTINGS.contains(key) || key.startsWith("blocks.")
        || key.startsWith("version.");
  }

  /** Returns whether a setting key (without {@code index.}) can be updated on a live index. */
  public static boolean isDynamicSetting(String key, Set<String> extraDynamicSettings) {
    if (DYNAMIC_SETTINGS.contains(key) || extraDynamicSettings.contains(key)) {
      return true;
    }
    for (String prefix : DYNAMIC_SETTING_PREFIXES) {
      if (key.startsWith(prefix)) {
        return true;
      }
    }
    return false;
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
   * @param dynamic the effective dynamic mode of the parent's current mapping
   * @param unindexed whether additions here are fields old documents were never indexed into
   * @param reportRemoved whether children only in {@code before} are reported as removed
   */
  private static void diffChildren(
      String prefix, Map<String, Object> before, Map<String, Object> after, String dynamic,
      boolean unindexed, boolean reportRemoved, Collector out) {
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
        diffNode(path, map(existing), map(entry.getValue()), dynamic, out);
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
      Map<String, Object> after, String inheritedDynamic, Collector out) {
    Object beforeType = before.get("type");
    Object afterType = after.get("type");
    if (!Objects.equals(beforeType, afterType)) {
      out.typeChanged.add(new Change(path, beforeType, afterType));
      return;
    }
    diffParams(path, before, after, out);
    String dynamic = before.containsKey("dynamic")
        ? String.valueOf(before.get("dynamic")) : inheritedDynamic;
    if (!isOpenEnded(after)) {
      Map<String, Object> beforeProperties = map(before.get("properties"));
      Map<String, Object> afterProperties = map(after.get("properties"));
      if (isOpenEnded(before)) {
        // Typed properties over a dynamic object: keys the engine picked up are not removals,
        // and additions were never indexed for the documents already there.
        diffChildren(path + ".", beforeProperties, afterProperties, dynamic, true, false, out);
      } else {
        boolean dynamicContainer = isContainer(before) && DEFAULT_DYNAMIC.equals(dynamic)
            && beforeProperties.isEmpty();
        diffChildren(path + ".", beforeProperties, afterProperties, dynamic, dynamicContainer,
            true, out);
      }
    }
    // A multi-field added to an existing field is empty for every document indexed before it.
    diffChildren(path + ".", map(before.get("fields")), map(after.get("fields")), dynamic, true,
        true, out);
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

  /**
   * Flattens settings to dotted keys without the {@code index.} prefix, whether they come nested
   * ({@code {"index": {"analysis": {...}}}}) or flat ({@code "index.refresh_interval"}).
   */
  static Map<String, Object> flattenSettings(Map<String, Object> settings) {
    Map<String, Object> flat = new TreeMap<>();
    if (settings != null) {
      flattenInto("", settings, flat);
    }
    return flat;
  }

  private static void flattenInto(String prefix, Map<String, Object> node,
      Map<String, Object> flat) {
    for (Map.Entry<String, Object> entry : node.entrySet()) {
      String key = prefix + entry.getKey();
      if (key.equals("index") && entry.getValue() instanceof Map) {
        flattenInto("", map(entry.getValue()), flat);
        continue;
      }
      if (key.startsWith("index.")) {
        key = key.substring("index.".length());
      }
      if (entry.getValue() instanceof Map) {
        flattenInto(key + ".", map(entry.getValue()), flat);
      } else {
        flat.put(key, entry.getValue());
      }
    }
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
