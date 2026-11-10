package com.phatjam98.elasticprotos.migration;

import com.phatjam98.elasticprotos.migration.MappingDiff.Change;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The index settings the proto declares that differ from the index's current settings.
 *
 * <p>Keys are dotted, without the {@code index.} prefix ({@code refresh_interval}, {@code
 * analysis.analyzer.folding.tokenizer}). Only keys the proto (and its overrides) declares are
 * compared, as strings; a setting removed from the proto is never reset. See {@link
 * DiffClassifier#DYNAMIC_SETTINGS} for which keys are dynamic.
 *
 * @param dynamicChanges settings the engine can update on a live index; {@code before} is the
 *     current value or null, {@code after} the desired value as declared
 * @param staticChanges settings that can only be set when an index is created
 * @param ignored keys the engine or the migrator manages ({@code blocks.*}, {@code uuid}, {@code
 *     creation_date}, {@code version.*}, {@code provided_name}), never applied
 */
public record SettingsDiff(
    List<Change> dynamicChanges, List<Change> staticChanges, List<String> ignored) {

  /** Copies the lists. */
  public SettingsDiff {
    dynamicChanges = List.copyOf(dynamicChanges);
    staticChanges = List.copyOf(staticChanges);
    ignored = List.copyOf(ignored);
  }

  /** Returns a diff without differences. */
  public static SettingsDiff empty() {
    return new SettingsDiff(List.of(), List.of(), List.of());
  }

  /** Returns whether no dynamic or static setting differs. */
  public boolean isEmpty() {
    return dynamicChanges.isEmpty() && staticChanges.isEmpty();
  }

  /**
   * Returns the desired values of the dynamic changes keyed {@code index.<key>}, ready for {@code
   * PUT /<index>/_settings}.
   */
  public Map<String, Object> dynamicValues() {
    Map<String, Object> values = new LinkedHashMap<>();
    for (Change change : dynamicChanges) {
      values.put("index." + change.path(), change.after());
    }
    return values;
  }

  /** Describes every change, for example {@code refresh_interval 1s -> 5s}. */
  public String describe() {
    List<Change> all = new ArrayList<>(dynamicChanges);
    all.addAll(staticChanges);
    return describe(all);
  }

  /** Describes changes as {@code key before -> after}, comma separated. */
  static String describe(List<Change> changes) {
    List<String> parts = new ArrayList<>();
    for (Change change : changes) {
      parts.add(change.path() + " " + change.before() + " -> " + change.after());
    }
    return String.join(", ", parts);
  }
}
