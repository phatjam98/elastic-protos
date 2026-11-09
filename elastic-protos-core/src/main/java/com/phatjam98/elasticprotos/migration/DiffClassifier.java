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

  /** Root mapping parameter holding the dynamic templates of typed maps. */
  private static final String DYNAMIC_TEMPLATES = "dynamic_templates";

  /** Name prefix of the dynamic templates the mapper emits. */
  private static final String TEMPLATE_PREFIX = "ep:";

  /** The dynamic mode of a mapping that does not declare one. */
  private static final String DEFAULT_DYNAMIC = "true";

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
    String dynamic = before.containsKey("dynamic")
        ? String.valueOf(before.get("dynamic")) : DEFAULT_DYNAMIC;
    diffChildren("", map(before.get("properties")), map(after.get("properties")), dynamic,
        false, true, out);
    return new MappingDiff(out.added, out.removed, out.typeChanged, out.paramChanged,
        settingsChanged(existingSettings, desiredSettings), out.unindexed);
  }

  /**
   * Classifies a diff.
   *
   * <p>Type changes and changes to any parameter not in {@link #UPDATABLE_PARAMS} (except root
   * {@code dynamic_templates} changes described at {@code templateChange}) need a reindex; removed
   * fields need one only under {@link RemovedFieldPolicy#REINDEX}. Otherwise, {@link
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
    boolean putParams = false;
    for (Change change : diff.paramChanged()) {
      if (UPDATABLE_PARAMS.contains(param(change.path()))) {
        putParams = true;
        continue;
      }
      TemplateChange templates = templateChange(change, diff, removedFields);
      if (templates == TemplateChange.REINDEX) {
        return Action.REINDEX;
      }
      putParams |= templates == TemplateChange.PUT;
    }
    if (!diff.removed().isEmpty() && removedFields == RemovedFieldPolicy.REINDEX) {
      return Action.REINDEX;
    }
    if (!diff.unindexedAdditions().isEmpty()) {
      return Action.BACKFILL;
    }
    if (!diff.added().isEmpty() || putParams) {
      return Action.PUT_MAPPING;
    }
    return Action.NO_OP;
  }

  /** How a change to the root {@code dynamic_templates} is applied. */
  private enum TemplateChange {
    /** Only templates of removed typed maps went away: nothing to apply. */
    NONE,
    /** Putting the mapping, which replaces the whole template list, is enough. */
    PUT,
    /** Documents already indexed would not follow the new templates. */
    REINDEX
  }

  /**
   * Classifies a change to the root {@code dynamic_templates}; any other parameter change is a
   * reindex. Templates are compared by name ({@code ep:P.*...}, where {@code P} is the typed map):
   *
   * <ul>
   *   <li>an added template is applied in place when its map is added, or when the map's values
   *       were already typed: documents of such a map were written without the new value field. An
   *       untyped map (root object template only) that becomes typed is a reindex, since its keys
   *       hold types the engine guessed;
   *   <li>a changed template is applied in place only when its map {@code P}, or an object
   *       enclosing it, is itself added by the diff;
   *   <li>a removed template is ignored when its map, or an object enclosing it, is removed and
   *       removed fields are kept ({@link RemovedFieldPolicy#KEEP}), like any removed field.
   * </ul>
   */
  private static TemplateChange templateChange(Change change, MappingDiff diff,
      RemovedFieldPolicy removedFields) {
    if (!(ROOT + ":" + DYNAMIC_TEMPLATES).equals(change.path())) {
      return TemplateChange.REINDEX;
    }
    Map<String, Object> before = templatesByName(change.before());
    Map<String, Object> after = templatesByName(change.after());
    boolean put = false;
    for (Map.Entry<String, Object> entry : after.entrySet()) {
      String map = mapOf(entry.getKey());
      if (map == null) {
        return TemplateChange.REINDEX;
      }
      if (!before.containsKey(entry.getKey())) {
        if (!covers(diff.added(), map) && !typedBefore(before, map)) {
          // an untyped map (root template only) turning typed: its keys hold guessed types
          return TemplateChange.REINDEX;
        }
        put = true;
      } else if (!Objects.equals(before.get(entry.getKey()), entry.getValue())) {
        if (!covers(diff.added(), map)) {
          return TemplateChange.REINDEX;
        }
        put = true;
      }
    }
    for (String name : before.keySet()) {
      if (after.containsKey(name)) {
        continue;
      }
      String map = mapOf(name);
      if (map == null || removedFields == RemovedFieldPolicy.REINDEX
          || !covers(diff.removed(), map)) {
        return TemplateChange.REINDEX;
      }
    }
    return put ? TemplateChange.PUT : TemplateChange.NONE;
  }

  /**
   * Returns whether the current templates typed the values of a map: they hold a template below
   * the root {@code ep:P.*} or a root template for single values (one not limited to objects).
   */
  private static boolean typedBefore(Map<String, Object> before, String map) {
    String root = TEMPLATE_PREFIX + map + ".*";
    for (Map.Entry<String, Object> entry : before.entrySet()) {
      if (entry.getKey().startsWith(root + ".")) {
        return true;
      }
      if (entry.getKey().equals(root) && !map(entry.getValue()).containsKey("match_mapping_type")) {
        return true;
      }
    }
    return false;
  }

  /** Returns the typed map path {@code P} of a template named {@code ep:P.*...}, or null. */
  private static String mapOf(String templateName) {
    int star = templateName.indexOf(".*");
    return templateName.startsWith(TEMPLATE_PREFIX) && star > TEMPLATE_PREFIX.length()
        ? templateName.substring(TEMPLATE_PREFIX.length(), star)
        : null;
  }

  /** Returns whether one of the changes is the map path itself or an object enclosing it. */
  private static boolean covers(List<Change> changes, String map) {
    for (Change change : changes) {
      if (change.path().equals(map) || map.startsWith(change.path() + ".")) {
        return true;
      }
    }
    return false;
  }

  private static Map<String, Object> templatesByName(Object templates) {
    Map<String, Object> byName = new TreeMap<>();
    if (templates instanceof List) {
      for (Object template : (List<?>) templates) {
        byName.putAll(map(template));
      }
    }
    return byName;
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
