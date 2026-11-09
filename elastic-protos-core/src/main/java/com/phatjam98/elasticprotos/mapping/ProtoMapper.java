package com.phatjam98.elasticprotos.mapping;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.phatjam98.elasticprotos.options.OptionsReader;
import com.phatjam98.elasticprotos.proto.Bool3;
import com.phatjam98.elasticprotos.proto.DynamicMode;
import com.phatjam98.elasticprotos.proto.EsType;
import com.phatjam98.elasticprotos.proto.FieldOpts;
import com.phatjam98.elasticprotos.proto.MessageOpts;
import com.phatjam98.elasticprotos.util.Json;
import com.phatjam98.elasticprotos.util.Names;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Turns a protobuf message descriptor into an {@link IndexDefinition}.
 *
 * <p>Mapping decisions are made in three layers, each overriding the previous: the
 * {@link MappingDefaults}, the {@code elastic} and {@code elastic_field} options declared in the
 * proto, and the {@link MappingOverrides} supplied programmatically. A field's
 * {@code raw_mapping_json} is merged last.
 *
 * <p>The mapper works from descriptors, so it serves generated classes and
 * {@code DynamicMessage} schemas alike, and it is deterministic: the same descriptor and
 * configuration always produce the same mappings and the same content hash.
 */
public final class ProtoMapper {

  /** Version of the mapping rules, recorded in {@code _meta}; bumped on incompatible changes. */
  public static final int SCHEMA_VERSION = 1;

  /** Key of the library's entry inside the mapping's {@code _meta}. */
  public static final String META_KEY = "elastic_protos";

  private static final String TIMESTAMP = "google.protobuf.Timestamp";
  private static final String LAT_LON = "phatjam98.elasticprotos.v1.LatLon";
  private static final String GEO_SHAPE = "phatjam98.elasticprotos.v1.GeoShape";
  private static final Pattern VALID_NAME = Pattern.compile("^[^.\\s]+$");
  private static final String NESTED_LIMIT = "index.mapping.nested_fields.limit";
  private static final String TOTAL_LIMIT = "index.mapping.total_fields.limit";
  private static final int DEFAULT_MAX_SHINGLE_SIZE = 3;
  private static final int MAX_INDEX_NAME_BYTES = 255;
  private static final Set<String> LAT_NAMES = Set.of("lat", "latitude");
  private static final Set<String> LON_NAMES = Set.of("lon", "lng", "longitude");
  private static final Set<String> STRUCT_LIKE =
      Set.of("google.protobuf.Struct", "google.protobuf.Value", "google.protobuf.ListValue");
  private static final Set<String> WRAPPERS =
      Set.of(
          "google.protobuf.StringValue",
          "google.protobuf.Int32Value",
          "google.protobuf.Int64Value",
          "google.protobuf.UInt32Value",
          "google.protobuf.UInt64Value",
          "google.protobuf.BoolValue",
          "google.protobuf.DoubleValue",
          "google.protobuf.FloatValue",
          "google.protobuf.BytesValue");

  private final MappingDefaults defaults;
  private final IndexNamingStrategy naming;
  private final MappingOverrides overrides;

  private ProtoMapper(Builder builder) {
    this.defaults = builder.defaults;
    this.naming = builder.naming;
    this.overrides = builder.overrides;
  }

  /** Returns a mapper with {@link MappingDefaults#standard()} and no overrides. */
  public static ProtoMapper withDefaults() {
    return builder().build();
  }

  /** Starts building a mapper. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns the defaults this mapper applies. */
  public MappingDefaults defaults() {
    return defaults;
  }

  /** Returns the naming strategy this mapper applies. */
  public IndexNamingStrategy naming() {
    return naming;
  }

  /**
   * Maps a generated message class.
   *
   * @param type a generated protobuf message class
   * @return the index definition
   */
  public IndexDefinition map(Class<? extends Message> type) {
    return map(descriptorOf(type));
  }

  /**
   * Maps a message descriptor with this mapper's overrides.
   *
   * @param descriptor the root message
   * @return the index definition
   * @throws MappingException if the message is marked {@code skip} or declares an invalid option
   */
  public IndexDefinition map(Descriptor descriptor) {
    return map(descriptor, overrides);
  }

  /**
   * Maps a message descriptor with explicit overrides, ignoring the mapper's own.
   *
   * @param descriptor the root message
   * @param explicitOverrides overrides for this call
   * @return the index definition
   * @throws MappingException if the message is marked {@code skip} or declares an invalid option
   */
  public IndexDefinition map(Descriptor descriptor, MappingOverrides explicitOverrides) {
    return new Run(descriptor, explicitOverrides).run();
  }

  /**
   * Returns whether a message declares {@code skip: TRUE} in its proto options. Programmatic
   * overrides are not consulted; use {@link #skips(Descriptor)} for the decision this mapper makes.
   *
   * @param descriptor the message
   * @return true if the proto marks it skipped
   */
  public static boolean isSkipped(Descriptor descriptor) {
    return OptionsReader.messageOpts(descriptor).getSkip() == Bool3.TRUE;
  }

  /**
   * Returns whether this mapper leaves a message without an index: its proto declares {@code skip:
   * TRUE} and no override sets {@code skip} to {@code FALSE}, or an override sets it to {@code
   * TRUE}.
   *
   * @param descriptor the message
   * @return true if it should not get an index
   */
  public boolean skips(Descriptor descriptor) {
    return mergedMessageOpts(descriptor, overrides).getSkip() == Bool3.TRUE;
  }

  private static MessageOpts mergedMessageOpts(Descriptor descriptor, MappingOverrides overrides) {
    MessageOpts declared = OptionsReader.messageOpts(descriptor);
    return overrides
        .message(descriptor)
        .map(override -> declared.toBuilder().mergeFrom(override).build())
        .orElse(declared);
  }

  /**
   * Returns the descriptor of a generated message class.
   *
   * @param type a generated protobuf message class
   * @return its descriptor
   * @throws IllegalArgumentException if the class has no static {@code getDescriptor()}
   */
  public static Descriptor descriptorOf(Class<? extends Message> type) {
    try {
      return (Descriptor) type.getMethod("getDescriptor").invoke(null);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException(
          type.getName() + " is not a generated protobuf message class", e);
    }
  }

  /** Builder for {@link ProtoMapper}. */
  public static final class Builder {

    private MappingDefaults defaults = MappingDefaults.standard();
    private IndexNamingStrategy naming = IndexNamingStrategy.DEFAULT;
    private MappingOverrides overrides = MappingOverrides.none();

    private Builder() {}

    /** Sets the defaults. */
    public Builder defaults(MappingDefaults value) {
      this.defaults = value;
      return this;
    }

    /** Sets the naming strategy. */
    public Builder naming(IndexNamingStrategy value) {
      this.naming = value;
      return this;
    }

    /** Sets the overrides applied by {@link ProtoMapper#map(Descriptor)}. */
    public Builder overrides(MappingOverrides value) {
      this.overrides = value;
      return this;
    }

    /** Builds the mapper. */
    public ProtoMapper build() {
      return new ProtoMapper(this);
    }
  }

  /** State for one mapping run. */
  private final class Run {

    private final Descriptor root;
    private final MappingOverrides runOverrides;
    private final MessageOpts messageOpts;
    private final Map<String, FieldInfo> fields = new TreeMap<>();
    private final Set<String> nestedPaths = new TreeSet<>();
    private final Map<String, Float> fullTextFields = new TreeMap<>();
    private final Set<String> ignoredPaths = new TreeSet<>();
    private final List<String> warnings = new ArrayList<>();
    private final Deque<Descriptor> ancestors = new ArrayDeque<>();
    private String idField;

    Run(Descriptor root, MappingOverrides runOverrides) {
      this.root = root;
      this.runOverrides = runOverrides;
      this.messageOpts = mergedMessageOpts(root, runOverrides);
    }

    IndexDefinition run() {
      if (messageOpts.getSkip() == Bool3.TRUE) {
        throw new MappingException(root.getFullName(), "message is marked skip = TRUE");
      }
      String declaredAlias =
          messageOpts.getAlias().isEmpty()
              ? (messageOpts.getIndex().isEmpty()
                  ? Names.snakeCase(root.getName())
                  : messageOpts.getIndex())
              : messageOpts.getAlias();
      declaredAlias = declaredName("alias", declaredAlias);
      final String declaredBase = messageOpts.getIndex().isEmpty()
          ? declaredAlias : declaredName("index", messageOpts.getIndex());

      ancestors.push(root);
      final Map<String, Object> properties = properties(root, "", "");
      ancestors.pop();
      final Map<String, Object> settings = settings();
      checkLimits(properties, settings);

      if (!messageOpts.getIdField().isEmpty()) {
        if (!fields.containsKey(messageOpts.getIdField())) {
          throw new MappingException(
              root.getFullName(),
              "id_field '" + messageOpts.getIdField() + "' is not a mapped field");
        }
        idField = messageOpts.getIdField();
      }

      Map<String, Object> mappings = new TreeMap<>();
      mappings.put("dynamic", dynamicValue(messageOpts.getDynamic()));
      mappings.put("properties", properties);
      String hash = Json.sha256Hex(mappings).substring(0, 8);
      Map<String, Object> meta = new TreeMap<>();
      meta.put("hash", hash);
      meta.put("proto", root.getFullName());
      meta.put("schema_version", SCHEMA_VERSION);
      mappings.put("_meta", Map.of(META_KEY, meta));

      String alias = naming.alias(declaredAlias);
      String name = naming.indexName(declaredBase, hash);
      int bytes = name.getBytes(StandardCharsets.UTF_8).length;
      if (bytes > MAX_INDEX_NAME_BYTES) {
        throw new MappingException(root.getFullName(), "index name " + name + " is " + bytes
            + " bytes, more than the " + MAX_INDEX_NAME_BYTES + " an index name may have");
      }
      return new IndexDefinition(
          root,
          alias,
          name,
          hash,
          settings,
          mappings,
          fields,
          nestedPaths,
          fullTextFields,
          ignoredPaths,
          idField,
          warnings);
    }

    /** Lower-cases a declared alias or index name and checks it can name an index. */
    private String declaredName(String option, String value) {
      try {
        return IndexNames.check(option, value);
      } catch (IllegalArgumentException e) {
        throw new MappingException(root.getFullName(), e.getMessage());
      }
    }

    /**
     * Rejects mappings the engine would refuse under its field limits: more {@code nested} fields,
     * or more fields in total, than the index settings allow ({@code
     * index.mapping.nested_fields.limit}, {@code index.mapping.total_fields.limit}) or, where the
     * settings declare none, than {@link MappingDefaults#maxNestedFields()} and {@link
     * MappingDefaults#maxTotalFields()}.
     */
    private void checkLimits(Map<String, Object> properties, Map<String, Object> settings) {
      int[] counts = new int[2];
      count(properties, counts);
      check(counts[0], "nested fields", settingLimit(settings, NESTED_LIMIT),
          defaults.maxNestedFields(), "maxNestedFields", NESTED_LIMIT);
      check(counts[1], "fields", settingLimit(settings, TOTAL_LIMIT),
          defaults.maxTotalFields(), "maxTotalFields", TOTAL_LIMIT);
    }

    private void check(int count, String what, Integer fromSettings, int fromDefaults,
        String option, String setting) {
      if (fromSettings != null) {
        if (fromSettings > 0 && count > fromSettings) {
          throw new MappingException(root.getFullName(), "maps " + count + " " + what
              + ", more than " + setting + " " + fromSettings + " in the index settings");
        }
        return;
      }
      if (fromDefaults > 0 && count > fromDefaults) {
        throw new MappingException(root.getFullName(), "maps " + count + " " + what
            + ", more than " + option + " " + fromDefaults + " (" + setting + ")");
      }
    }

    /** Reads a limit from settings written nested, dotted, or with or without {@code index.}. */
    private Integer settingLimit(Map<String, Object> settings, String key) {
      Object value = flatten("", settings).get(key);
      if (value == null) {
        return null;
      }
      try {
        return Integer.valueOf(value.toString().trim());
      } catch (NumberFormatException e) {
        throw new MappingException(root.getFullName(), key + " is not a number: " + value);
      }
    }

    private Map<String, Object> settings() {
      Map<String, Object> settings = new TreeMap<>();
      if (messageOpts.getShards() > 0) {
        settings.put("number_of_shards", messageOpts.getShards());
      }
      if (messageOpts.getHasReplicas()) {
        settings.put("number_of_replicas", messageOpts.getReplicas());
      }
      if (!messageOpts.getSettingsJson().isEmpty()) {
        settings =
            Json.deepMerge(
                settings, parseJson(messageOpts.getSettingsJson(), root.getFullName()));
      }
      if (!runOverrides.settings().isEmpty()) {
        settings = Json.deepMerge(settings, runOverrides.settings());
      }
      return settings;
    }

    private Map<String, Object> properties(
        Descriptor descriptor, String pathPrefix, String protoPathPrefix) {
      Map<String, Object> properties = new TreeMap<>();
      for (FieldDescriptor field : descriptor.getFields()) {
        String protoPath = join(protoPathPrefix, field.getName());
        FieldOpts options =
            OptionsReader.fieldOpts(field).toBuilder()
                .mergeFrom(runOverrides.field(field, protoPath))
                .build();
        if (options.getType() == EsType.IGNORE) {
          ignoredPaths.add(protoPath);
          continue;
        }
        String esName = options.getName().isEmpty() ? field.getName() : options.getName();
        if (!VALID_NAME.matcher(esName).matches()) {
          throw new MappingException(protoPath, "elastic_field.name '" + esName
              + "' must be non-empty and contain no '.' or whitespace");
        }
        if (!options.getName().isEmpty() && esName.startsWith("_")) {
          throw new MappingException(protoPath, "elastic_field.name '" + esName
              + "' must not start with '_', which is reserved for metadata fields");
        }
        String path = join(pathPrefix, esName);
        Map<String, Object> property = property(field, options, path, protoPath);
        if (property == null) {
          continue;
        }
        if (properties.containsKey(esName)) {
          throw new MappingException(path, "duplicate field name in the index");
        }
        properties.put(esName, property);
      }
      return properties;
    }

    private Map<String, Object> property(
        FieldDescriptor field, FieldOpts options, String path, String protoPath) {
      FieldKind kind = kindOf(field, options, path);
      if (kind == null) {
        ignoredPaths.add(protoPath);
        warnings.add(
            path + ": " + field.getMessageType().getFullName()
                + " has no index representation and is ignored");
        return null;
      }

      Map<String, Object> property = new TreeMap<>();
      boolean keywordSubfield = false;
      int ignoreAbove =
          options.getIgnoreAbove() > 0 ? options.getIgnoreAbove() : defaults.ignoreAbove();
      switch (kind) {
        case KEYWORD:
          property.put("ignore_above", ignoreAbove);
          if (!options.getNormalizer().isEmpty()) {
            property.put("normalizer", options.getNormalizer());
          }
          break;
        case TEXT:
        case SEARCH_AS_YOU_TYPE:
          String analyzer =
              options.getAnalyzer().isEmpty()
                  ? messageOpts.getDefaultAnalyzer()
                  : options.getAnalyzer();
          if (!analyzer.isEmpty()) {
            property.put("analyzer", analyzer);
          }
          if (!options.getSearchAnalyzer().isEmpty()) {
            property.put("search_analyzer", options.getSearchAnalyzer());
          }
          keywordSubfield =
              kind == FieldKind.TEXT
                  && resolve(options.getKeywordSubfield(), defaults.keywordSubfield());
          if (keywordSubfield) {
            Map<String, Object> keyword = new TreeMap<>();
            keyword.put("type", FieldKind.KEYWORD.esType());
            keyword.put("ignore_above", ignoreAbove);
            property.put("fields", Map.of("keyword", keyword));
          }
          break;
        case DATE:
        case DATE_NANOS:
          property.put(
              "format",
              options.getFormat().isEmpty() ? defaults.dateFormat() : options.getFormat());
          break;
        case OBJECT:
        case NESTED:
          if (field.isMapField() || isStructLike(field)) {
            property.put("dynamic", true);
          } else {
            Map<String, Object> children = descend(field.getMessageType(), path, protoPath);
            if (children == null) {
              kind = FieldKind.OBJECT;
              property.put("enabled", false);
            } else {
              property.put("properties", children);
            }
          }
          if (kind == FieldKind.NESTED) {
            nestedPaths.add(path);
          }
          break;
        default:
          break;
      }
      property.put("type", kind.esType());

      if (options.getIndex() == Bool3.FALSE) {
        property.put("index", false);
      }
      if (options.getDocValues() == Bool3.FALSE) {
        property.put("doc_values", false);
      }
      if (options.getCoerce() != Bool3.UNSET && kind.isNumeric()) {
        property.put("coerce", options.getCoerce() == Bool3.TRUE);
      }
      if (options.getCopyToCount() > 0) {
        property.put("copy_to", new ArrayList<>(options.getCopyToList()));
      }
      if (!options.getNullValue().isEmpty()) {
        property.put("null_value", options.getNullValue());
      }
      if (!options.getRawMappingJson().isEmpty()) {
        property = Json.deepMerge(property, parseJson(options.getRawMappingJson(), path));
      }

      float boost = options.getBoost() > 0 ? options.getBoost() : 1.0f;
      fields.put(
          path,
          new FieldInfo(path, protoPath, kind, field.isRepeated(), keywordSubfield, boost, field));
      if (options.getFullText() == Bool3.TRUE
          || (kind.isText() && options.getFullText() != Bool3.FALSE)) {
        fullTextFields.put(path, boost);
      }
      if (options.getId() == Bool3.TRUE) {
        if (idField == null) {
          idField = path;
        } else if (!idField.equals(path)) {
          warnings.add(path + ": more than one field is marked id; using " + idField);
        }
      }
      return property;
    }

    private Map<String, Object> descend(Descriptor child, String path, String protoPath) {
      if (ancestors.contains(child)) {
        warnings.add(
            path + ": recursive message " + child.getFullName()
                + " is mapped as a disabled object");
        return null;
      }
      if (ancestors.size() > defaults.maxDepth()) {
        warnings.add(
            path + ": nesting deeper than " + defaults.maxDepth()
                + " is mapped as a disabled object");
        return null;
      }
      ancestors.push(child);
      try {
        return properties(child, path, protoPath);
      } finally {
        ancestors.pop();
      }
    }

    private FieldKind kindOf(FieldDescriptor field, FieldOpts options, String path) {
      if (options.getType() != EsType.AUTO) {
        FieldKind requested = FieldKind.fromEsType(options.getType());
        validate(requested, field, path);
        return requested;
      }
      return defaultKind(field, path);
    }

    private FieldKind defaultKind(FieldDescriptor field, String path) {
      if (field.isMapField()) {
        // Keys are data, not schema: flattened indexes them without growing the mapping.
        // EsType.OBJECT on the field opts in to an object with dynamic: true.
        return FieldKind.FLATTENED;
      }
      switch (field.getType()) {
        case DOUBLE:
          return FieldKind.DOUBLE;
        case FLOAT:
          return FieldKind.FLOAT;
        case INT64:
        case SFIXED64:
        case SINT64:
          return FieldKind.LONG;
        case UINT64:
        case FIXED64:
          warnings.add(path + ": " + (field.getType() == FieldDescriptor.Type.UINT64 ? "uint64"
              : "fixed64") + " does not fit a long and is mapped as keyword");
          return FieldKind.KEYWORD;
        case INT32:
        case SFIXED32:
        case SINT32:
          return FieldKind.INTEGER;
        case UINT32:
        case FIXED32:
          return FieldKind.LONG;
        case BOOL:
          return FieldKind.BOOLEAN;
        case STRING:
          return defaults.stringsAsText() ? FieldKind.TEXT : FieldKind.KEYWORD;
        case BYTES:
          return FieldKind.BINARY;
        case ENUM:
          return FieldKind.KEYWORD;
        default:
          return defaultMessageKind(field, path);
      }
    }

    private FieldKind defaultMessageKind(FieldDescriptor field, String path) {
      switch (field.getMessageType().getFullName()) {
        case TIMESTAMP:
          return FieldKind.DATE;
        case "google.protobuf.Duration":
          return FieldKind.KEYWORD;
        case "google.protobuf.StringValue":
          return defaults.stringsAsText() ? FieldKind.TEXT : FieldKind.KEYWORD;
        case "google.protobuf.Int32Value":
          return FieldKind.INTEGER;
        case "google.protobuf.Int64Value":
        case "google.protobuf.UInt32Value":
          return FieldKind.LONG;
        case "google.protobuf.UInt64Value":
          warnings.add(path + ": uint64 does not fit a long and is mapped as keyword");
          return FieldKind.KEYWORD;
        case "google.protobuf.BoolValue":
          return FieldKind.BOOLEAN;
        case "google.protobuf.DoubleValue":
          return FieldKind.DOUBLE;
        case "google.protobuf.FloatValue":
          return FieldKind.FLOAT;
        case "google.protobuf.BytesValue":
          return FieldKind.BINARY;
        case "google.protobuf.Struct":
        case "google.protobuf.Value":
        case "google.protobuf.ListValue":
          return defaults.structKind();
        case "google.protobuf.Any":
        case "google.protobuf.FieldMask":
        case "google.protobuf.Empty":
          return null;
        case LAT_LON:
          return FieldKind.GEO_POINT;
        case GEO_SHAPE:
          return FieldKind.GEO_SHAPE;
        default:
          return field.isRepeated() ? defaults.repeatedMessage() : defaults.singularMessage();
      }
    }

    private void validate(FieldKind kind, FieldDescriptor field, String path) {
      boolean message = field.getType() == FieldDescriptor.Type.MESSAGE && !isWrapper(field);
      String typeName = message ? field.getMessageType().getFullName() : field.getType().name();
      boolean string = field.getType() == FieldDescriptor.Type.STRING;
      switch (kind) {
        case OBJECT:
        case NESTED:
        case FLATTENED:
          if (!message) {
            throw new MappingException(path, kind + " requires a message or map field");
          }
          break;
        case GEO_POINT:
          if (!(string || (message && isLatLonMessage(field.getMessageType())))) {
            throw new MappingException(
                path,
                "GEO_POINT requires a string field (WKT or \"lat,lon\") or a message with lat/lon"
                    + " fields, not " + typeName);
          }
          break;
        case GEO_SHAPE:
          if (!(string || (message && GEO_SHAPE.equals(typeName)))) {
            throw new MappingException(
                path,
                "GEO_SHAPE requires a string field (WKT or GeoJSON) or " + GEO_SHAPE + ", not "
                    + typeName);
          }
          break;
        case DATE:
        case DATE_NANOS:
          if (message && !TIMESTAMP.equals(typeName)) {
            throw new MappingException(path, kind + " cannot be applied to " + typeName);
          }
          break;
        default:
          if (message) {
            throw new MappingException(path, kind + " cannot be applied to message " + typeName);
          }
          break;
      }
    }
  }

  /**
   * Counts the fields of a {@code properties} object the way the engines count them for their
   * field limits: every property, object and nested containers included, and every multi-field.
   *
   * @param counts {@code [nested fields, total fields]}, incremented in place
   */
  @SuppressWarnings("unchecked")
  private static void count(Map<String, Object> properties, int[] counts) {
    for (Object value : properties.values()) {
      if (!(value instanceof Map)) {
        continue;
      }
      Map<String, Object> property = (Map<String, Object>) value;
      counts[1] += fieldCount(property);
      if ("nested".equals(property.get("type"))) {
        counts[0]++;
      }
      if (property.get("properties") instanceof Map) {
        count((Map<String, Object>) property.get("properties"), counts);
      }
      if (property.get("fields") instanceof Map) {
        count((Map<String, Object>) property.get("fields"), counts);
      }
    }
  }

  /**
   * Returns how many fields one mapped property adds, not counting its children: a {@code
   * search_as_you_type} field adds its {@code _2gram} to {@code _<max_shingle_size>gram}
   * sub-fields and {@code _index_prefix} (4 at the default {@code max_shingle_size} of 3), and a
   * {@code text} field adds one for {@code index_prefixes} and one for {@code index_phrases}.
   */
  private static int fieldCount(Map<String, Object> property) {
    Object type = property.get("type");
    if ("search_as_you_type".equals(type)) {
      Object shingles = property.get("max_shingle_size");
      return 1 + (shingles == null
          ? DEFAULT_MAX_SHINGLE_SIZE : Integer.parseInt(shingles.toString()));
    }
    int count = 1;
    if ("text".equals(type)) {
      if (property.get("index_prefixes") != null) {
        count++;
      }
      if (Boolean.parseBoolean(String.valueOf(property.get("index_phrases")))) {
        count++;
      }
    }
    return count;
  }

  /** Flattens settings to {@code index.}-prefixed dotted keys. */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> flatten(String prefix, Map<String, Object> settings) {
    Map<String, Object> flat = new TreeMap<>();
    for (Map.Entry<String, Object> entry : settings.entrySet()) {
      String key = prefix + entry.getKey();
      if (entry.getValue() instanceof Map) {
        flat.putAll(flatten(key + ".", (Map<String, Object>) entry.getValue()));
      } else {
        flat.put(key.startsWith("index.") ? key : "index." + key, entry.getValue());
      }
    }
    return flat;
  }

  private static boolean isStructLike(FieldDescriptor field) {
    return field.getType() == FieldDescriptor.Type.MESSAGE
        && STRUCT_LIKE.contains(field.getMessageType().getFullName());
  }

  private static boolean isWrapper(FieldDescriptor field) {
    return field.getType() == FieldDescriptor.Type.MESSAGE
        && WRAPPERS.contains(field.getMessageType().getFullName());
  }

  private static boolean isLatLonMessage(Descriptor descriptor) {
    boolean lat = false;
    boolean lon = false;
    for (FieldDescriptor field : descriptor.getFields()) {
      boolean numeric =
          field.getType() == FieldDescriptor.Type.DOUBLE
              || field.getType() == FieldDescriptor.Type.FLOAT;
      lat |= numeric && LAT_NAMES.contains(field.getName());
      lon |= numeric && LON_NAMES.contains(field.getName());
    }
    return lat && lon;
  }

  private static boolean resolve(Bool3 value, boolean fallback) {
    return value == Bool3.UNSET ? fallback : value == Bool3.TRUE;
  }

  private Object dynamicValue(DynamicMode mode) {
    DynamicMode effective = mode == DynamicMode.DYNAMIC_DEFAULT ? defaults.dynamic() : mode;
    switch (effective) {
      case DYNAMIC_FALSE:
        return Boolean.FALSE;
      case DYNAMIC_TRUE:
        return Boolean.TRUE;
      default:
        return "strict";
    }
  }

  private static Map<String, Object> parseJson(String json, String path) {
    try {
      return Json.parseObject(json);
    } catch (IllegalArgumentException e) {
      throw new MappingException(path, "option holds invalid JSON: " + e.getMessage());
    }
  }

  private static String join(String prefix, String name) {
    return prefix.isEmpty() ? name : prefix + "." + name;
  }
}
