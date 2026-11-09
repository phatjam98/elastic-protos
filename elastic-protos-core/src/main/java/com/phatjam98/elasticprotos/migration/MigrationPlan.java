package com.phatjam98.elasticprotos.migration;

import com.phatjam98.elasticprotos.migration.MappingDiff.Change;
import com.phatjam98.elasticprotos.util.Json;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The decisions for a set of messages, in the order they were given. Apply it with {@link
 * IndexMigrator#apply(MigrationPlan)}, or print it for a dry run.
 *
 * @param items one plan per mapped message
 */
public record MigrationPlan(List<IndexPlan> items) {

  private static final Set<Action> CHANGES = Set.of(Action.CREATE, Action.PUT_MAPPING,
      Action.REINDEX);

  /** Copies the items. */
  public MigrationPlan {
    items = List.copyOf(items);
  }

  /** Returns whether applying the plan would change anything. */
  public boolean hasChanges() {
    return items.stream().anyMatch(item -> CHANGES.contains(item.action()));
  }

  /** Returns the items the migrator refuses to apply. */
  public List<IndexPlan> blocked() {
    return items.stream().filter(item -> item.action() == Action.BLOCKED).toList();
  }

  /**
   * Returns a human-readable summary: one line per index with its action, the indices involved
   * and the number of changed fields, followed by warnings and, for blocked and no-op items, the
   * reason. The indices read {@code current -> target} for {@link Action#CREATE}, {@link
   * Action#REINDEX} and {@link Action#BLOCKED}, and {@code current (in place)} for {@link
   * Action#PUT_MAPPING} and {@link Action#NO_OP}, whose live index keeps its name.
   */
  public String toReport() {
    StringBuilder out = new StringBuilder();
    out.append("Migration plan: ").append(items.size()).append(items.size() == 1 ? " index" :
        " indices").append(hasChanges() ? ", changes pending" : ", no changes").append('\n');
    for (IndexPlan item : items) {
      MappingDiff diff = item.diff();
      out.append(String.format("  %-11s %s: %s (added %d, removed %d, changed %d)%n",
          item.action(), item.alias(), indices(item),
          diff.added().size(), diff.removed().size(),
          diff.typeChanged().size() + diff.paramChanged().size()));
      for (String warning : item.warnings()) {
        out.append("      warning: ").append(warning).append('\n');
      }
      if (item.action() == Action.BLOCKED) {
        out.append("      blocked: ").append(item.reason()).append('\n');
      } else if (item.action() == Action.NO_OP) {
        out.append("      ").append(item.reason()).append('\n');
      }
    }
    return out.toString();
  }

  /**
   * Describes the indices an item touches: {@code current -> target} when a new index is created,
   * {@code current (in place)} when the live index is kept.
   */
  private static String indices(IndexPlan item) {
    String current = item.currentIndex() == null ? "(none)" : item.currentIndex();
    if (item.action() == Action.PUT_MAPPING || item.action() == Action.NO_OP) {
      return current + " (in place)";
    }
    return current + " -> " + item.target().name();
  }

  /** Returns the plan as pretty-printed JSON. */
  public String toJson() {
    List<Object> list = new ArrayList<>();
    for (IndexPlan item : items) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("alias", item.alias());
      entry.put("action", item.action().name());
      entry.put("currentIndex", item.currentIndex());
      entry.put("currentHash", item.currentHash());
      entry.put("targetIndex", item.target().name());
      entry.put("targetHash", item.target().contentHash());
      entry.put("reason", item.reason());
      entry.put("warnings", item.warnings());
      Map<String, Object> diff = new LinkedHashMap<>();
      diff.put("added", changes(item.diff().added()));
      diff.put("removed", changes(item.diff().removed()));
      diff.put("typeChanged", changes(item.diff().typeChanged()));
      diff.put("paramChanged", changes(item.diff().paramChanged()));
      diff.put("unindexedAdditions", changes(item.diff().unindexedAdditions()));
      diff.put("settingsChanged", item.diff().settingsChanged());
      entry.put("diff", diff);
      list.add(entry);
    }
    Map<String, Object> root = new LinkedHashMap<>();
    root.put("hasChanges", hasChanges());
    root.put("items", list);
    return Json.pretty(root);
  }

  private static List<Object> changes(List<Change> changes) {
    List<Object> list = new ArrayList<>();
    for (Change change : changes) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("path", change.path());
      entry.put("before", change.before());
      entry.put("after", change.after());
      list.add(entry);
    }
    return list;
  }
}
