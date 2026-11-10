package com.phatjam98.elasticprotos.query;

import com.phatjam98.elasticprotos.mapping.FieldInfo;
import com.phatjam98.elasticprotos.mapping.FieldKind;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.proto.Highlight;
import com.phatjam98.elasticprotos.proto.HighlightField;
import com.phatjam98.elasticprotos.util.Json;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
 * query whose path is {@code parties}. So every positive {@code nested} clause (in {@code filter},
 * {@code must} or {@code should}, not {@code must_not}) on the innermost nested path of a
 * highlighted field gets an {@code inner_hits} highlighting the fields of that path; several
 * clauses on one path each get their own, named {@code <path>__highlight}, {@code
 * <path>__highlight2}, ... so the names are unique. Each highlighted field carries a {@code
 * highlight_query} that ORs the queries of every positive clause on its path, so an object marks up
 * the same terms whichever clause returned it. An outer {@code nested} clause that encloses such a
 * clause gets an {@code inner_hits} without highlight, since the engines return inner hits of an
 * inner clause only inside the inner hits of the outer one. Inner hits carry no {@code _source};
 * {@code nested_objects} sets their size at every level.
 *
 * <p>A requested nested field is highlighted only when a positive clause on its path can produce
 * fragments for it: a full-text query (match, phrase or multi-match) on the field itself, or on any
 * field when {@code require_field_match} is false. Otherwise it is refused (skipped with a warning
 * when lenient); with the default field set and {@code include_nested}, such fields are skipped
 * silently. There is no query text on that path to highlight with; a carrier clause with a
 * highlight query taken from elsewhere would be possible, and may be added later as an option.
 */
final class HighlightCompiler {

  static final String DEFAULT_PRE_TAG = "<em>";
  static final String DEFAULT_POST_TAG = "</em>";

  /** Suffix of the inner hits names, after the nested path. */
  static final String INNER_HITS_SUFFIX = "__highlight";

  /** Queries whose terms the highlighter marks up in the fields they name. */
  private static final Set<String> TEXT_QUERIES =
      Set.of("match", "match_phrase", "match_phrase_prefix", "multi_match");

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
    if (highlight.getNestedObjects() < 0) {
      throw new QueryCompileException("highlight.nested_objects must not be negative: "
          + highlight.getNestedObjects(), null);
    }
    if (highlight.getNestedObjects() > config.maxInnerResultWindow()) {
      throw new QueryCompileException("highlight.nested_objects = "
          + highlight.getNestedObjects() + " exceeds the inner hits window of "
          + config.maxInnerResultWindow() + " (QueryCompilerConfig.maxInnerResultWindow, the"
          + " default index.max_inner_result_window); raise both to allow more", null);
    }
    Map<String, Object> rootFields = new TreeMap<>();
    Map<String, Map<String, Object>> nestedFields = new TreeMap<>();
    Set<String> requested = new HashSet<>();
    if (highlight.getFieldsCount() == 0) {
      for (String path : index.fullTextFields().keySet()) {
        FieldInfo field = lookup(path);
        if (field == null || !field.kind().isText() || typedMapNested(path) != null) {
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
      for (HighlightField field : highlight.getFieldsList()) {
        String path = field.getField();
        if (path.isEmpty()) {
          throw new QueryCompileException("Every highlight field needs a field name", null);
        }
        FieldInfo info = usable(path);
        if (info == null) {
          continue;
        }
        String nested = innermost(info);
        if (nested == null) {
          rootFields.put(path, options(path, field));
        } else {
          nestedFields.computeIfAbsent(nested, k -> new TreeMap<>())
              .put(path, options(path, field));
          requested.add(path);
        }
      }
    }
    if (rootFields.isEmpty() && nestedFields.isEmpty()) {
      warnings.add("Skipped highlight: no text field outside nested objects to highlight");
      return new Compiled(null, query);
    }
    Map<String, Object> settings = settings(highlight);
    Map<String, Object> withInnerHits = query;
    boolean attached = false;
    if (!nestedFields.isEmpty()) {
      withInnerHits = Json.sortedCopy(query);
      Map<String, List<Object>> clauses = new HashMap<>();
      collect(withInnerHits, clauses);
      boolean anyField = highlight.hasRequireFieldMatch() && !highlight.getRequireFieldMatch();
      for (Iterator<Map.Entry<String, Map<String, Object>>> paths =
          nestedFields.entrySet().iterator(); paths.hasNext(); ) {
        Map.Entry<String, Map<String, Object>> path = paths.next();
        List<Object> onPath = clauses.getOrDefault(path.getKey(), List.of());
        path.getValue().keySet().removeIf(field -> {
          if (onPath.stream().anyMatch(clause -> highlights(clause, field, anyField))) {
            return false;
          }
          if (requested.contains(field)) {
            missingClause(field, path.getKey(), !onPath.isEmpty(), anyField);
          }
          return true;
        });
        if (path.getValue().isEmpty()) {
          paths.remove();
          continue;
        }
        Object highlightQuery = onPath.size() == 1 ? onPath.get(0)
            : NestedPathResolver.object("bool", NestedPathResolver.object("should", onPath));
        for (Object options : path.getValue().values()) {
          @SuppressWarnings("unchecked")
          Map<String, Object> fieldOptions = (Map<String, Object>) options;
          fieldOptions.put("highlight_query", highlightQuery);
        }
      }
      if (!nestedFields.isEmpty()) {
        new InnerHits(nestedFields, settings, highlight.getNestedObjects())
            .attach(withInnerHits);
        attached = true;
      }
    }
    if (rootFields.isEmpty() && !attached) {
      warnings.add("Skipped highlight: no field to highlight; fields inside nested objects need"
          + " a full-text condition of the query on them inside their nested path");
      return new Compiled(null, query);
    }
    Map<String, Object> body = null;
    if (!rootFields.isEmpty()) {
      body = new TreeMap<>(settings);
      body.put("fields", rootFields);
    }
    return new Compiled(body, attached ? withInnerHits : query);
  }

  private void missingClause(String field, String nestedPath, boolean hasClause,
      boolean anyField) {
    String problem = "highlight on '" + field + "' needs a full-text condition of the query"
        + (anyField ? "" : " on that field") + " (TEXT_MATCHES, TEXT_CONTAINS or FULL_TEXT)"
        + " inside nested path '" + nestedPath + "': the engines highlight nested fields only"
        + " within the inner hits of a nested query on that path, with that query's conditions"
        + (hasClause ? ", and the conditions on '" + nestedPath + "' cannot mark up '" + field
            + "'" : ", and the query has none on '" + nestedPath + "'");
    if (!config.lenient()) {
      throw new QueryCompileException(problem, field);
    }
    warnings.add("Skipped " + problem);
  }

  /**
   * Collects deep copies of the queries of the positive {@code nested} clauses by path, before
   * any inner hits are attached, for the highlight queries.
   */
  private static void collect(Object clause, Map<String, List<Object>> clauses) {
    if (!(clause instanceof Map<?, ?> map)) {
      return;
    }
    if (map.get("nested") instanceof Map<?, ?> nested) {
      clauses.computeIfAbsent(String.valueOf(nested.get("path")), k -> new ArrayList<>())
          .add(copy(nested.get("query")));
      collect(nested.get("query"), clauses);
      return;
    }
    if (map.get("bool") instanceof Map<?, ?> bool) {
      for (String occur : List.of("filter", "must", "should")) {
        Object clauses2 = bool.get(occur);
        if (clauses2 instanceof List<?> list) {
          list.forEach(item -> collect(item, clauses));
        } else {
          collect(clauses2, clauses);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Object copy(Object query) {
    return query instanceof Map<?, ?> map ? Json.sortedCopy((Map<String, Object>) map) : query;
  }

  /**
   * Whether a clause can produce fragments for a field: it holds a full-text query on the field,
   * or on any field when the field need not match. Negated parts count too; they are rare and only
   * make a highlight query broader.
   */
  private static boolean highlights(Object clause, String field, boolean anyField) {
    if (clause instanceof List<?> list) {
      return list.stream().anyMatch(item -> highlights(item, field, anyField));
    }
    if (!(clause instanceof Map<?, ?> map)) {
      return false;
    }
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (TEXT_QUERIES.contains(String.valueOf(entry.getKey()))
          && entry.getValue() instanceof Map<?, ?> body) {
        if (anyField || targets(body, field)) {
          return true;
        }
      } else if (highlights(entry.getValue(), field, anyField)) {
        return true;
      }
    }
    return false;
  }

  /** Whether the body of a match-family query names the field. */
  private static boolean targets(Map<?, ?> body, String field) {
    if (body.containsKey(field)) {
      return true;
    }
    if (body.get("fields") instanceof List<?> fields) {
      for (Object name : fields) {
        String plain = String.valueOf(name);
        int boost = plain.indexOf('^');
        if ((boost < 0 ? plain : plain.substring(0, boost)).equals(field)) {
          return true;
        }
      }
    }
    return false;
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
    return body;
  }

  /** Returns the innermost nested path enclosing a field, or null outside nested objects. */
  private String innermost(FieldInfo field) {
    List<String> ancestors = index.nestedAncestors(field.path());
    return ancestors.isEmpty() ? null : ancestors.get(ancestors.size() - 1);
  }

  /**
   * Returns the nested pattern path of a typed map that a field pattern such as {@code
   * folders.*.pages.text} lies inside, or null. Such patterns cannot be highlighted: the nested
   * path differs per key, and inner hits need a concrete one.
   */
  private String typedMapNested(String path) {
    if (!path.contains("*")) {
      return null;
    }
    for (FieldInfo dynamic : index.dynamicFields().values()) {
      if (dynamic.kind() == FieldKind.NESTED && path.startsWith(dynamic.path() + ".")) {
        return dynamic.path();
      }
    }
    return null;
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
    String problem = null;
    String nestedPattern = typedMapNested(path);
    if (!field.kind().isText()) {
      problem = "highlight needs a text field but '" + path + "' is mapped as " + field.kind();
    } else if (nestedPattern != null) {
      problem = "highlight cannot use pattern '" + path + "' inside the nested values '"
          + nestedPattern + "' of a typed map: their nested path differs per key; name a key,"
          + " such as '" + path.replaceFirst("\\*", "<key>") + "', and query that path";
    }
    if (problem == null) {
      return field;
    }
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
    private final Map<String, Integer> names = new HashMap<>();

    InnerHits(Map<String, Map<String, Object>> fieldsByPath, Map<String, Object> settings,
        int size) {
      this.fieldsByPath = fieldsByPath;
      this.settings = settings;
      this.size = size;
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
