package com.phatjam98.elasticprotos.query;

import static com.phatjam98.elasticprotos.query.NestedPathResolver.object;

import com.phatjam98.elasticprotos.mapping.FieldInfo;
import com.phatjam98.elasticprotos.mapping.FieldKind;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.proto.DistanceUnit;
import com.phatjam98.elasticprotos.proto.FieldSort;
import com.phatjam98.elasticprotos.proto.GeoDistanceSort;
import com.phatjam98.elasticprotos.proto.ScriptSort;
import com.phatjam98.elasticprotos.proto.ScriptSortType;
import com.phatjam98.elasticprotos.proto.SortingCriteria;
import com.phatjam98.elasticprotos.proto.SortingOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/** Compiles {@link SortingCriteria} into the {@code sort} array of a search body. */
final class SortCompiler {

  private final QueryCompilerConfig config;
  private final IndexDefinition index;
  private final List<String> warnings;

  SortCompiler(QueryCompilerConfig config, IndexDefinition index, List<String> warnings) {
    this.config = config;
    this.index = index;
    this.warnings = warnings;
  }

  List<Object> compile(List<SortingCriteria> criteria) {
    List<Object> sort = new ArrayList<>();
    for (SortingCriteria criterion : criteria) {
      Map<String, Object> clause = compile(criterion);
      if (clause != null) {
        sort.add(clause);
      }
    }
    return sort;
  }

  private Map<String, Object> compile(SortingCriteria criterion) {
    switch (criterion.getSortTypeCase()) {
      case FIELD_SORT:
        return field(criterion.getFieldSort());
      case SCORE_SORT:
        return object("_score", order(criterion.getScoreSort().getOrder()));
      case GEO_DISTANCE_SORT:
        return geoDistance(criterion.getGeoDistanceSort());
      case SCRIPT_SORT:
        return script(criterion.getScriptSort());
      default:
        throw new QueryCompileException("A sorting criterion has no sort type set", null);
    }
  }

  private Map<String, Object> field(FieldSort sort) {
    String name = sort.getFieldName();
    if (name.startsWith("_")) {
      // metadata fields such as _id, _doc or _shard_doc pass through unchanged
      return object(name, order(sort.getOrder()));
    }
    FieldInfo field = index.resolve(name).orElse(null);
    if (field == null) {
      return unknown(name);
    }
    rejectDynamicNested(field);
    if (!field.supportsExactMatch()) {
      throw new QueryCompileException(
          "Cannot sort on '" + name + "': it is analyzed text without a keyword sub-field; add "
              + "[(elastic_field).keyword_subfield = TRUE] to the field",
          name);
    }
    FieldKind kind = field.kind();
    if (kind.isContainer() || kind == FieldKind.GEO_POINT || kind == FieldKind.GEO_SHAPE) {
      throw new QueryCompileException(
          "Cannot sort on '" + name + "' mapped as " + kind
              + (kind == FieldKind.GEO_POINT ? "; use a geo distance sort" : ""),
          name);
    }
    Map<String, Object> spec = order(sort.getOrder());
    nested(spec, name);
    if (field.isDynamic()) {
      // a key no document has used yet is not mapped, and sorting on an unmapped field fails
      spec.put("unmapped_type", field.exactMatchPath().equals(field.path())
          ? kind.esType() : FieldKind.KEYWORD.esType());
    }
    return object(field.exactMatchPath(), spec);
  }

  private Map<String, Object> geoDistance(GeoDistanceSort sort) {
    String name = sort.getFieldName();
    FieldInfo field = index.resolve(name).orElse(null);
    if (field == null) {
      return unknown(name);
    }
    rejectDynamicNested(field);
    if (field.kind() != FieldKind.GEO_POINT) {
      throw new QueryCompileException(
          "Geo distance sort needs a GEO_POINT field but '" + name + "' is mapped as "
              + field.kind(),
          name);
    }
    Map<String, Object> spec = order(sort.getOrder());
    Map<String, Object> point = new TreeMap<>();
    point.put("lat", sort.getPoint().getLat());
    point.put("lon", sort.getPoint().getLon());
    spec.put(field.path(), point);
    spec.put("unit", unit(sort.getDistanceUnit()));
    spec.put("distance_type", sort.getDistanceType().name().toLowerCase(Locale.ROOT));
    if (field.isDynamic()) {
      spec.put("ignore_unmapped", true);
    }
    nested(spec, name);
    return object("_geo_distance", spec);
  }

  /**
   * Compiles a script sort: inline {@code source} as Painless, or a stored script by {@code id},
   * which carries its own language.
   */
  private Map<String, Object> script(ScriptSort sort) {
    if (!config.allowScriptSort()) {
      throw new QueryCompileException(
          "Script sorts are disabled: they run a client-supplied script on the cluster; enable"
              + " QueryCompilerConfig.allowScriptSort to accept them",
          null);
    }
    Map<String, Object> script = new TreeMap<>();
    switch (sort.getScriptCase()) {
      case SOURCE:
        if (sort.getSource().isEmpty()) {
          throw new QueryCompileException("A script sort has an empty source", null);
        }
        script.put("lang", "painless");
        script.put("source", sort.getSource());
        break;
      case ID:
        if (sort.getId().isEmpty()) {
          throw new QueryCompileException("A script sort has an empty stored script id", null);
        }
        script.put("id", sort.getId());
        break;
      default:
        throw new QueryCompileException(
            "A script sort needs a script: set source (inline Painless) or id (stored script)",
            null);
    }
    Map<String, Object> spec = order(sort.getOrder());
    spec.put("type", sort.getSortType() == ScriptSortType.STRING ? "string" : "number");
    spec.put("script", script);
    return object("_script", spec);
  }

  /**
   * Rejects a sort on a field inside the values of a typed map below a nested path: the engines
   * need the nested path mapped to sort, and it is mapped only once a document uses the key.
   */
  private void rejectDynamicNested(FieldInfo field) {
    if (!field.isDynamic()) {
      return;
    }
    for (String nested : index.nestedAncestors(field.path())) {
      if (index.resolve(nested).map(FieldInfo::isDynamic).orElse(false)) {
        throw new QueryCompileException("Cannot sort on '" + field.path()
            + "': sorting under a typed nested map is not supported", field.path());
      }
    }
  }

  private Map<String, Object> unknown(String name) {
    if (!config.lenient()) {
      throw new QueryCompileException(
          "Unknown sort field '" + name + "'; index " + index.alias() + " does not map it",
          name);
    }
    warnings.add("Skipped sort on unknown field '" + name + "'");
    return null;
  }

  /**
   * Adds {@code nested} for a field inside nested objects. Deeper levels are expressed as a chain
   * of {@code nested} objects, outermost first, as the engines require.
   */
  private void nested(Map<String, Object> spec, String path) {
    List<String> ancestors = index.nestedAncestors(path);
    Map<String, Object> inner = null;
    for (int i = ancestors.size() - 1; i >= 0; i--) {
      Map<String, Object> level = new TreeMap<>();
      level.put("path", ancestors.get(i));
      if (inner != null) {
        level.put("nested", inner);
      }
      inner = level;
    }
    if (inner != null) {
      spec.put("nested", inner);
    }
  }

  private static Map<String, Object> order(SortingOrder order) {
    Map<String, Object> spec = new TreeMap<>();
    spec.put("order", order == SortingOrder.DESC ? "desc" : "asc");
    return spec;
  }

  static String unit(DistanceUnit unit) {
    switch (unit) {
      case INCH:
        return "in";
      case YARD:
        return "yd";
      case FEET:
        return "ft";
      case NAUTICALMILES:
        return "nmi";
      case MILLIMETERS:
        return "mm";
      case CENTIMETERS:
        return "cm";
      case MILES:
        return "mi";
      case METERS:
        return "m";
      default:
        return "km";
    }
  }
}
