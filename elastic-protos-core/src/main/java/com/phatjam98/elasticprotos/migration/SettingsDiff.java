package com.phatjam98.elasticprotos.migration;

import com.phatjam98.elasticprotos.migration.MappingDiff.Change;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The index settings the proto declares that differ from the index's current settings, and the
 * dynamic settings it declared before and no longer does.
 *
 * <p>Keys are dotted, without the {@code index.} prefix ({@code refresh_interval}, {@code
 * analysis.analyzer.folding.tokenizer}). Keys the proto (and its overrides) declares are compared,
 * as strings. A dynamic key the index records as declared ({@code
 * _meta.elastic_protos.dynamic_settings}) that the proto no longer declares is reset. See {@link
 * DiffClassifier#DYNAMIC_SETTINGS} for which keys are dynamic.
 *
 * @param dynamicChanges settings the engine can update on a live index; {@code before} is the
 *     current value or null, {@code after} the desired value as declared
 * @param staticChanges settings that can only be set when an index is created
 * @param ignored keys the engine or the migrator manages ({@code blocks.*}, {@code uuid}, {@code
 *     creation_date}, {@code version.*}, {@code provided_name}), never applied
 * @param resets dynamic settings the proto declared and no longer does; {@code before} is the
 *     current value or null, {@code after} is null (the engine default), or for {@code
 *     number_of_replicas} the count the replica policy gives a new index
 */
public record SettingsDiff(List<Change> dynamicChanges, List<Change> staticChanges,
    List<String> ignored, List<Change> resets) {

  /** Copies the lists. */
  public SettingsDiff {
    dynamicChanges = List.copyOf(dynamicChanges);
    staticChanges = List.copyOf(staticChanges);
    ignored = List.copyOf(ignored);
    resets = List.copyOf(resets);
  }

  /**
   * Creates a diff without resets.
   *
   * @param dynamicChanges settings the engine can update on a live index
   * @param staticChanges settings that can only be set when an index is created
   * @param ignored keys the engine or the migrator manages
   */
  public SettingsDiff(List<Change> dynamicChanges, List<Change> staticChanges,
      List<String> ignored) {
    this(dynamicChanges, staticChanges, ignored, List.of());
  }

  /** Returns a diff without differences. */
  public static SettingsDiff empty() {
    return new SettingsDiff(List.of(), List.of(), List.of(), List.of());
  }

  /** Returns whether no dynamic or static setting differs and nothing is reset. */
  public boolean isEmpty() {
    return dynamicChanges.isEmpty() && staticChanges.isEmpty() && resets.isEmpty();
  }

  /** Returns whether a dynamic setting is to be updated or reset on the live index. */
  public boolean hasDynamicWork() {
    return !dynamicChanges.isEmpty() || !resets.isEmpty();
  }

  /**
   * Returns the desired values of the dynamic changes and the resets keyed {@code index.<key>},
   * ready for {@code PUT /<index>/_settings}; a reset to the engine default is a null value.
   */
  public Map<String, Object> dynamicValues() {
    Map<String, Object> values = new LinkedHashMap<>();
    for (Change change : dynamicChanges) {
      values.put("index." + change.path(), change.after());
    }
    for (Change change : resets) {
      values.put("index." + change.path(), change.after());
    }
    return values;
  }

  /**
   * Describes the dynamic changes and the resets, for example {@code refresh_interval 5s ->
   * default}.
   */
  public String describeDynamic() {
    List<Change> all = new ArrayList<>(dynamicChanges);
    all.addAll(resets);
    return describe(all);
  }

  /** Describes every change, for example {@code refresh_interval 1s -> 5s}. */
  public String describe() {
    List<Change> all = new ArrayList<>(dynamicChanges);
    all.addAll(resets);
    all.addAll(staticChanges);
    return describe(all);
  }

  /**
   * Describes changes as {@code key before -> after}, comma separated; a null {@code after} (a
   * reset) reads {@code default}.
   */
  static String describe(List<Change> changes) {
    List<String> parts = new ArrayList<>();
    for (Change change : changes) {
      parts.add(change.path() + " " + change.before() + " -> "
          + (change.after() == null ? "default" : change.after()));
    }
    return String.join(", ", parts);
  }
}
