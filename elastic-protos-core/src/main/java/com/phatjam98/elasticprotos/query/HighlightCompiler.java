package com.phatjam98.elasticprotos.query;

import com.phatjam98.elasticprotos.mapping.FieldInfo;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.proto.Highlight;
import com.phatjam98.elasticprotos.proto.HighlightField;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Compiles a {@link Highlight} into the {@code highlight} object of a search body. Only text
 * fields outside nested objects are highlighted: fragments of nested fields would need {@code
 * inner_hits}.
 */
final class HighlightCompiler {

  static final String DEFAULT_PRE_TAG = "<em>";
  static final String DEFAULT_POST_TAG = "</em>";

  private final QueryCompilerConfig config;
  private final IndexDefinition index;
  private final List<String> warnings;

  HighlightCompiler(QueryCompilerConfig config, IndexDefinition index, List<String> warnings) {
    this.config = config;
    this.index = index;
    this.warnings = warnings;
  }

  /**
   * Compiles the highlight settings.
   *
   * @return the {@code highlight} object, or null when no field can be highlighted
   */
  Map<String, Object> compile(Highlight highlight) {
    Map<String, Object> fields = new TreeMap<>();
    if (highlight.getFieldsCount() == 0) {
      for (String path : index.fullTextFields().keySet()) {
        FieldInfo field = lookup(path);
        if (field != null && field.kind().isText()
            && index.nestedAncestors(field.path()).isEmpty()) {
          fields.put(path, new TreeMap<>());
        }
      }
    } else {
      for (HighlightField requested : highlight.getFieldsList()) {
        String path = requested.getField();
        if (path.isEmpty()) {
          throw new QueryCompileException("Every highlight field needs a field name", null);
        }
        if (usable(path)) {
          fields.put(path, options(path, requested));
        }
      }
    }
    if (fields.isEmpty()) {
      warnings.add("Skipped highlight: no text field outside nested objects to highlight");
      return null;
    }
    Map<String, Object> body = new TreeMap<>();
    body.put("fields", fields);
    body.put("pre_tags", List.of(highlight.getPreTag().isEmpty()
        ? DEFAULT_PRE_TAG : highlight.getPreTag()));
    body.put("post_tags", List.of(highlight.getPostTag().isEmpty()
        ? DEFAULT_POST_TAG : highlight.getPostTag()));
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

  /** A declared field, a typed-map key, or a typed-map pattern such as {@code notes.*}. */
  private FieldInfo lookup(String path) {
    FieldInfo field = index.dynamicFields().get(path);
    return field != null ? field : index.resolve(path).orElse(null);
  }

  private boolean usable(String path) {
    FieldInfo field = lookup(path);
    String problem = null;
    if (field == null) {
      if (!config.lenient()) {
        throw ConditionCompiler.unknownField(index, path, "highlight");
      }
      warnings.add("Skipped highlight on unknown field '" + path + "'");
      return false;
    }
    if (!field.kind().isText()) {
      problem = "highlight needs a text field but '" + path + "' is mapped as " + field.kind();
    } else if (!index.nestedAncestors(field.path()).isEmpty()) {
      problem = "highlight cannot use '" + path + "' inside nested path '"
          + index.nestedAncestors(field.path()).get(0)
          + "'; fragments of nested fields need inner_hits";
    }
    if (problem == null) {
      return true;
    }
    if (!config.lenient()) {
      throw new QueryCompileException(problem, path);
    }
    warnings.add("Skipped " + problem);
    return false;
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
}
