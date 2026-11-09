package com.phatjam98.elasticprotos.query;

import com.phatjam98.elasticprotos.mapping.FieldInfo;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Wraps clauses on fields inside {@code nested} objects in {@code nested} queries and assembles
 * the {@code bool} query of one condition.
 *
 * <p>Only the paths the index maps as {@code nested} cause wrapping; dotted paths through plain
 * {@code object} fields are left alone. A field under several nested levels is wrapped once per
 * level, outermost first.
 *
 * <p>With grouping enabled, the clauses of one condition whose innermost nested path is the same
 * are combined into a single {@code nested} query whose inner {@code bool} keeps the
 * filter/must/must_not split, so that they must all match the same nested element. A group is
 * placed in {@code must} when it contains a scoring clause and in {@code filter} otherwise. A
 * group made only of negated clauses is not combined: each stays a {@code must_not} of its own
 * {@code nested} query, meaning "no element matches". Groups with different innermost paths are
 * wrapped independently, even when one path is inside the other.
 *
 * <p>Internal: this class is an implementation detail of {@link QueryCompiler}, is not part of the
 * public API and may change in a minor release.
 */
public final class NestedPathResolver {

  /** Where a clause goes in a {@code bool} query. */
  public enum Occur {
    /** Non-scoring, required. */
    FILTER("filter"),
    /** Scoring, required. */
    MUST("must"),
    /** Excluded. */
    MUST_NOT("must_not");

    private final String key;

    Occur(String key) {
      this.key = key;
    }

    /** Returns the {@code bool} key for this occurrence. */
    public String key() {
      return key;
    }
  }

  /**
   * One predicate of a condition, before nesting is applied.
   *
   * @param occur where the clause goes
   * @param nestedPath the nested paths enclosing the clause, outermost first; empty when the
   *     clause is top-level or already wrapped
   * @param query the unwrapped query clause
   */
  public record Clause(Occur occur, List<String> nestedPath, Map<String, Object> query) {

    /** Copies the nested path. */
    public Clause {
      Objects.requireNonNull(occur, "occur");
      nestedPath = List.copyOf(nestedPath);
      Objects.requireNonNull(query, "query");
    }

    /** Returns the innermost nested path, or null for a top-level clause. */
    public String innermost() {
      return nestedPath.isEmpty() ? null : nestedPath.get(nestedPath.size() - 1);
    }
  }

  private final IndexDefinition index;
  private final boolean groupNested;

  /**
   * Creates a resolver.
   *
   * @param index the index whose nested paths apply
   * @param groupNested whether to group clauses that share their innermost nested path
   */
  public NestedPathResolver(IndexDefinition index, boolean groupNested) {
    this.index = Objects.requireNonNull(index, "index");
    this.groupNested = groupNested;
  }

  /** Returns the nested paths enclosing a field, outermost first. */
  public List<String> ancestors(String fieldPath) {
    return index.nestedAncestors(fieldPath);
  }

  /** Returns the innermost nested path enclosing a field, or null when it is top-level. */
  public String innermost(String fieldPath) {
    List<String> ancestors = ancestors(fieldPath);
    return ancestors.isEmpty() ? null : ancestors.get(ancestors.size() - 1);
  }

  /** Wraps a clause on a field in one {@code nested} query per enclosing nested path. */
  public Map<String, Object> wrap(String fieldPath, Map<String, Object> query) {
    return wrapPaths(ancestors(fieldPath), query);
  }

  /**
   * Wraps a clause in {@code nested} queries.
   *
   * @param nestedPath nested paths, outermost first
   * @param query the clause
   * @return the clause itself when the list is empty, otherwise the outermost {@code nested} query
   */
  public static Map<String, Object> wrap(List<String> nestedPath, Map<String, Object> query) {
    Map<String, Object> wrapped = query;
    for (int i = nestedPath.size() - 1; i >= 0; i--) {
      wrapped = nested(nestedPath.get(i), wrapped, false);
    }
    return wrapped;
  }

  /**
   * Wraps a clause in {@code nested} queries like {@link #wrap(List, Map)}, adding {@code
   * "ignore_unmapped": true} for nested paths inside the values of a typed map: such a path is
   * mapped only once a document uses its key, and a {@code nested} query on an unmapped path
   * fails.
   *
   * @param nestedPath nested paths, outermost first
   * @param query the clause
   * @return the clause itself when the list is empty, otherwise the outermost {@code nested} query
   */
  public Map<String, Object> wrapPaths(List<String> nestedPath, Map<String, Object> query) {
    Map<String, Object> wrapped = query;
    for (int i = nestedPath.size() - 1; i >= 0; i--) {
      wrapped = nested(nestedPath.get(i), wrapped,
          index.resolve(nestedPath.get(i)).map(FieldInfo::isDynamic).orElse(false));
    }
    return wrapped;
  }

  private static Map<String, Object> nested(String path, Map<String, Object> query,
      boolean ignoreUnmapped) {
    Map<String, Object> nested = new TreeMap<>();
    nested.put("path", path);
    nested.put("query", query);
    if (ignoreUnmapped) {
      nested.put("ignore_unmapped", true);
    }
    return object("nested", nested);
  }

  /**
   * Builds the {@code bool} query of one condition from its clauses.
   *
   * @param clauses the clauses in order; must not be empty
   * @return {@code {"bool": {...}}} with empty occurrence lists left out
   */
  public Map<String, Object> bool(List<Clause> clauses) {
    Map<Occur, List<Object>> out = new EnumMap<>(Occur.class);
    if (!groupNested) {
      for (Clause clause : clauses) {
        add(out, clause.occur(), wrapPaths(clause.nestedPath(), clause.query()));
      }
      return render(out);
    }
    Map<String, List<Clause>> groups = new LinkedHashMap<>();
    for (Clause clause : clauses) {
      if (clause.innermost() != null) {
        groups.computeIfAbsent(clause.innermost(), k -> new ArrayList<>()).add(clause);
      }
    }
    Set<String> emitted = new HashSet<>();
    for (Clause clause : clauses) {
      String key = clause.innermost();
      if (key == null) {
        add(out, clause.occur(), clause.query());
      } else if (emitted.add(key)) {
        emitGroup(out, groups.get(key));
      }
    }
    return render(out);
  }

  private void emitGroup(Map<Occur, List<Object>> out, List<Clause> group) {
    boolean anyPositive = group.stream().anyMatch(c -> c.occur() != Occur.MUST_NOT);
    if (group.size() == 1 || !anyPositive) {
      for (Clause clause : group) {
        add(out, clause.occur(), wrapPaths(clause.nestedPath(), clause.query()));
      }
      return;
    }
    Map<Occur, List<Object>> inner = new EnumMap<>(Occur.class);
    for (Clause clause : group) {
      add(inner, clause.occur(), clause.query());
    }
    Occur occur = inner.containsKey(Occur.MUST) ? Occur.MUST : Occur.FILTER;
    add(out, occur, wrapPaths(group.get(0).nestedPath(), render(inner)));
  }

  private static void add(Map<Occur, List<Object>> out, Occur occur, Object query) {
    out.computeIfAbsent(occur, k -> new ArrayList<>()).add(query);
  }

  private static Map<String, Object> render(Map<Occur, List<Object>> out) {
    Map<String, Object> bool = new TreeMap<>();
    for (Map.Entry<Occur, List<Object>> entry : out.entrySet()) {
      bool.put(entry.getKey().key(), entry.getValue());
    }
    return object("bool", bool);
  }

  static Map<String, Object> object(String key, Object value) {
    Map<String, Object> map = new TreeMap<>();
    map.put(key, value);
    return map;
  }
}
