package com.phatjam98.elasticprotos.query;

import com.phatjam98.elasticprotos.mapping.FieldInfo;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.proto.Highlight;
import com.phatjam98.elasticprotos.proto.HighlightField;
import com.phatjam98.elasticprotos.util.Json;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Compiles a {@link Highlight} into the {@code highlight} object of a search body and, for text
 * fields inside nested objects, into {@code inner_hits} on the query's {@code nested} clauses.
 *
 * <p>The engines highlight a nested field only within inner hits: the fragments of {@code
 * parties.name} come from an {@code inner_hits} with a {@code highlight} on a {@code nested}
 * query whose path is {@code parties}, and they highlight what that query's own clauses matched.
 * So every positive {@code nested} clause (in {@code filter}, {@code must} or {@code should}, not
 * {@code must_not}) on the innermost nested path of a highlighted field gets an {@code
 * inner_hits} highlighting the fields of that path; several clauses on one path each get their
 * own, named {@code <path>__highlight}, {@code <path>__highlight2}, ... so the names are unique.
 * An outer {@code nested} clause that encloses such a clause gets an {@code inner_hits} without
 * highlight, since the engines return inner hits of an inner clause only inside the inner hits of
 * the outer one. Inner hits carry no {@code _source}.
 *
 * <p>When the query has no positive clause on the nested path of an explicitly requested field,
 * there is nothing the engine could highlight there: a clause added only to carry inner hits would
 * match nothing to mark up (or, with {@code match_all}, every object, with no terms to highlight),
 * and would change scoring or cost. Such a field is refused (skipped with a warning when lenient);
 * with the default field set and {@code include_nested}, such paths are skipped silently.
 */
final class HighlightCompiler {

  static final String DEFAULT_PRE_TAG = "<em>";
  static final String DEFAULT_POST_TAG = "</em>";

  /** Suffix of the inner hits names, after the nested path. */
  static final String INNER_HITS_SUFFIX = "__highlight";

  private final QueryCompilerConfig config;
  private final IndexDefinition index;
  private final List<String> warnings;

  HighlightCompiler(QueryCompilerConfig config, IndexDefinition index, List<String> warnings) {
    this.config = config;
    this.index = index;
    this.warnings = warnings;
  }

  /**
   * The compiled highlighting.
   *
   * @param highlight the {@code highlight} object for fields outside nested objects, or null
   * @param query the query, with {@code inner_hits} added for nested fields; the given query
   *     itself when there are none
   */
  record Compiled(Map<String, Object> highlight, Map<String, Object> query) {}

  /**
   * Compiles the highlight settings against the compiled query.
   *
   * @param highlight the settings
   * @param query the compiled query, which is not modified
   * @return the root highlight and the query
   */
  Compiled compile(Highlight highlight, Map<String, Object> query) {
    Map<String, Object> rootFields = new TreeMap<>();
    Map<String, Map<String, Object>> nestedFields = new TreeMap<>();
    Map<String, String> requestedByPath = new TreeMap<>();
    if (highlight.getFieldsCount() == 0) {
      for (String path : index.fullTextFields().keySet()) {
        FieldInfo field = lookup(path);
        if (field == null || !field.kind().isText()) {
          continue;
        }
        String nested = innermost(field);
        if (nested == null) {
          rootFields.put(path, new TreeMap<>());
        } else if (highlight.getIncludeNested()) {
          nestedFields.computeIfAbsent(nested, k -> new TreeMap<>()).put(path, new TreeMap<>());
        }
      }
    } else {
      for (HighlightField requested : highlight.getFieldsList()) {
        String path = requested.getField();
        if (path.isEmpty()) {
          throw new QueryCompileException("Every highlight field needs a field name", null);
        }
        FieldInfo field = usable(path);
        if (field == null) {
          continue;
        }
        String nested = innermost(field);
        if (nested == null) {
          rootFields.put(path, options(path, requested));
        } else {
          nestedFields.computeIfAbsent(nested, k -> new TreeMap<>())
              .put(path, options(path, requested));
          requestedByPath.putIfAbsent(nested, path);
        }
      }
    }
    if (rootFields.isEmpty() && nestedFields.isEmpty()) {
      warnings.add("Skipped highlight: no text field outside nested objects to highlight");
      return new Compiled(null, query);
    }
    Map<String, Object> settings = settings(highlight);
    Map<String, Object> withInnerHits = query;
    Set<String> attached = new HashSet<>();
    if (!nestedFields.isEmpty()) {
      withInnerHits = Json.sortedCopy(query);
      new InnerHits(nestedFields, settings, highlight.getNestedObjects(), attached)
          .attach(withInnerHits);
      for (Map.Entry<String, String> requested : requestedByPath.entrySet()) {
        if (!attached.contains(requested.getKey())) {
          missingClause(requested.getValue(), requested.getKey());
        }
      }
    }
    if (rootFields.isEmpty() && attached.isEmpty()) {
      warnings.add("Skipped highlight: no field to highlight; fields inside nested objects need"
          + " a condition of the query on their nested path");
      return new Compiled(null, query);
    }
    Map<String, Object> body = null;
    if (!rootFields.isEmpty()) {
      body = new TreeMap<>(settings);
      body.put("fields", rootFields);
    }
    return new Compiled(body, withInnerHits);
  }

  private void missingClause(String field, String nestedPath) {
    String problem = "highlight on '" + field + "' needs a condition of the query on nested path '"
        + nestedPath + "': the engines highlight nested fields only within the inner hits of a"
        + " nested query on that path";
    if (!config.lenient()) {
      throw new QueryCompileException(problem, field);
    }
    warnings.add("Skipped " + problem);
  }

  /** The settings shared by the root highlight and every inner hits highlight. */
  private static Map<String, Object> settings(Highlight highlight) {
    Map<String, Object> body = new TreeMap<>();
    body.put("pre_tags", highlight.getPreTagsCount() == 0 ? List.of(DEFAULT_PRE_TAG)
        : List.copyOf(highlight.getPreTagsList()));
    body.put("post_tags", highlight.getPostTagsCount() == 0 ? List.of(DEFAULT_POST_TAG)
        : List.copyOf(highlight.getPostTagsList()));
    if (highlight.getFragmentSize() < 0) {
      throw new QueryCompileException(
          "highlight.fragment_size must not be negative: " + highlight.getFragmentSize(), null);
    }
    if (highlight.getFragmentSize() > 0) {
      body.put("fragment_size", highlight.getFragmentSize());
    }
    if (highlight.hasNumberOfFragments()) {
      body.put("number_of_fragments", fragments(highlight.getNumberOfFragments(), null));
    }
    if (highlight.hasRequireFieldMatch()) {
      body.put("require_field_match", highlight.getRequireFieldMatch());
    }
    if (highlight.getHtmlEncode()) {
      body.put("encoder", "html");
    }
    if (highlight.getNestedObjects() < 0) {
      throw new QueryCompileException("highlight.nested_objects must not be negative: "
          + highlight.getNestedObjects(), null);
    }
    return body;
  }

  /** Returns the innermost nested path enclosing a field, or null outside nested objects. */
  private String innermost(FieldInfo field) {
    List<String> ancestors = index.nestedAncestors(field.path());
    return ancestors.isEmpty() ? null : ancestors.get(ancestors.size() - 1);
  }

  /** A declared field, a typed-map key, or a typed-map pattern such as {@code notes.*}. */
  private FieldInfo lookup(String path) {
    FieldInfo field = index.dynamicFields().get(path);
    return field != null ? field : index.resolve(path).orElse(null);
  }

  /** Returns the field when it can be highlighted; throws or warns and returns null otherwise. */
  private FieldInfo usable(String path) {
    FieldInfo field = lookup(path);
    if (field == null) {
      if (!config.lenient()) {
        throw ConditionCompiler.unknownField(index, path, "highlight");
      }
      warnings.add("Skipped highlight on unknown field '" + path + "'");
      return null;
    }
    if (field.kind().isText()) {
      return field;
    }
    String problem =
        "highlight needs a text field but '" + path + "' is mapped as " + field.kind();
    if (!config.lenient()) {
      throw new QueryCompileException(problem, path);
    }
    warnings.add("Skipped " + problem);
    return null;
  }

  private static Map<String, Object> options(String path, HighlightField requested) {
    Map<String, Object> options = new TreeMap<>();
    if (requested.getFragmentSize() < 0) {
      throw new QueryCompileException("fragment_size of highlight field '" + path
          + "' must not be negative: " + requested.getFragmentSize(), path);
    }
    if (requested.getFragmentSize() > 0) {
      options.put("fragment_size", requested.getFragmentSize());
    }
    if (requested.hasNumberOfFragments()) {
      options.put("number_of_fragments", fragments(requested.getNumberOfFragments(), path));
    }
    return options;
  }

  private static int fragments(int value, String path) {
    if (value < 0) {
      throw new QueryCompileException("number_of_fragments"
          + (path == null ? "" : " of highlight field '" + path + "'")
          + " must not be negative: " + value, path);
    }
    return value;
  }

  /** Attaches {@code inner_hits} to the positive {@code nested} clauses of a query. */
  private static final class InnerHits {

    private final Map<String, Map<String, Object>> fieldsByPath;
    private final Map<String, Object> settings;
    private final int size;
    private final Set<String> attached;
    private final Map<String, Integer> names = new HashMap<>();

    InnerHits(Map<String, Map<String, Object>> fieldsByPath, Map<String, Object> settings,
        int size, Set<String> attached) {
      this.fieldsByPath = fieldsByPath;
      this.settings = settings;
      this.size = size;
      this.attached = attached;
    }

    /**
     * Walks a query clause; returns whether inner hits were attached in it, so that the enclosing
     * nested clause can carry them.
     */
    boolean attach(Object clause) {
      if (!(clause instanceof Map<?, ?> map)) {
        return false;
      }
      if (map.get("nested") instanceof Map<?, ?> nested) {
        return nested(cast(nested));
      }
      if (map.get("bool") instanceof Map<?, ?> bool) {
        boolean any = false;
        for (String occur : List.of("filter", "must", "should")) {
          Object clauses = bool.get(occur);
          if (clauses instanceof List<?> list) {
            for (Object item : list) {
              any |= attach(item);
            }
          } else {
            any |= attach(clauses);
          }
        }
        return any;
      }
      return false;
    }

    private boolean nested(Map<String, Object> nested) {
      String path = String.valueOf(nested.get("path"));
      boolean below = attach(nested.get("query"));
      Map<String, Object> fields = fieldsByPath.get(path);
      if (fields == null && !below) {
        return false;
      }
      Map<String, Object> innerHits = new TreeMap<>();
      int count = names.merge(path, 1, Integer::sum);
      innerHits.put("name", path + INNER_HITS_SUFFIX + (count == 1 ? "" : String.valueOf(count)));
      innerHits.put("_source", false);
      if (size > 0) {
        innerHits.put("size", size);
      }
      if (fields != null) {
        Map<String, Object> highlight = new TreeMap<>(settings);
        highlight.put("fields", Json.sortedCopy(fields));
        innerHits.put("highlight", highlight);
        attached.add(path);
      }
      nested.put("inner_hits", innerHits);
      return true;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> cast(Map<?, ?> map) {
      return (Map<String, Object>) map;
    }
  }
}
