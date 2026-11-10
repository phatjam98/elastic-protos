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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;
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
  private static final Pattern VALID_NAME = Pattern.compile("^[^.*\\s]+$");
  private static final Set<String> LAT_NAMES = Set.of("lat", "latitude");
  private static final Set<String> LON_NAMES = Set.of("lon", "lng", "longitude");
  private static final Set<String> STRUCT_LIKE =
      Set.of("google.protobuf.Struct", "google.protobuf.Value", "google.protobuf.ListValue");
  private static final Set<String> LOOSE_VALUES =
      Set.of("google.protobuf.Value", "google.protobuf.ListValue");
  private static final Set<String> UNTYPED_VALUES =
      Set.of("google.protobuf.Any", "google.protobuf.FieldMask", "google.protobuf.Empty");
  private static final Set<String> SINGLE_VALUE_MESSAGES =
      Set.of(TIMESTAMP, "google.protobuf.Duration", LAT_LON, GEO_SHAPE);
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
    private final Set<String> ignoredPaths = new TreeSet<>();
    private final List<String> warnings = new ArrayList<>();
    private final Deque<Descriptor> ancestors = new ArrayDeque<>();
    private final Map<String, FieldInfo> dynamicFields = new TreeMap<>();
    private final Map<String, Map<String, Object>> templates = new TreeMap<>();
    // Swapped for scratch collectors while the values of a typed map are mapped.
    private Map<String, FieldInfo> fields = new TreeMap<>();
    private Set<String> nestedPaths = new TreeSet<>();
    private Map<String, Float> fullTextFields = new TreeMap<>();
    private String idField;
    // How many typed map values enclose the field being mapped; 0 outside typed maps.
    private int mapValueDepth;

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
      final String declaredBase =
          messageOpts.getIndex().isEmpty() ? declaredAlias : messageOpts.getIndex();

      ancestors.push(root);
      final Map<String, Object> properties = properties(root, "", "");
      ancestors.pop();
      checkLimits(properties);

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
      if (!templates.isEmpty()) {
        mappings.put("dynamic_templates", dynamicTemplates());
      }
      String hash = Json.sha256Hex(mappings).substring(0, 8);
      Map<String, Object> meta = new TreeMap<>();
      meta.put("hash", hash);
      meta.put("proto", root.getFullName());
      meta.put("schema_version", SCHEMA_VERSION);
      mappings.put("_meta", Map.of(META_KEY, meta));

      String alias = naming.alias(declaredAlias);
      return new IndexDefinition(
          root,
          alias,
          naming.indexName(declaredBase, hash),
          hash,
          settings(),
          mappings,
          fields,
          nestedPaths,
          fullTextFields,
          ignoredPaths,
          idField,
          warnings,
          dynamicFields);
    }

    /**
     * Returns the dynamic templates in the order the engines must try them: the engines apply the
     * first template that matches and {@code *} in {@code path_match} also matches dots, so deeper
     * patterns come first; ties are ordered by name, which keeps the content hash stable.
     */
    private List<Object> dynamicTemplates() {
      List<String> names = new ArrayList<>(templates.keySet());
      names.sort(Comparator.comparingInt((String name) -> -segments(name))
          .thenComparing(Comparator.naturalOrder()));
      List<Object> list = new ArrayList<>();
      for (String name : names) {
        Map<String, Object> template = new TreeMap<>();
        template.put(name, templates.get(name));
        list.add(template);
      }
      return list;
    }

    /**
     * Rejects mappings the engine would refuse under its default field limits: more {@code
     * nested} fields than {@link MappingDefaults#maxNestedFields()}, or more fields in total than
     * {@link MappingDefaults#maxTotalFields()}.
     */
    private void checkLimits(Map<String, Object> properties) {
      int[] counts = new int[2];
      count(properties, counts);
      int nested = counts[0];
      int total = counts[1];
      if (defaults.maxNestedFields() > 0 && nested > defaults.maxNestedFields()) {
        throw new MappingException(root.getFullName(), "maps " + nested
            + " nested fields, more than maxNestedFields " + defaults.maxNestedFields()
            + " (index.mapping.nested_fields.limit)");
      }
      if (defaults.maxTotalFields() > 0 && total > defaults.maxTotalFields()) {
        throw new MappingException(root.getFullName(), "maps " + total
            + " fields, more than maxTotalFields " + defaults.maxTotalFields()
            + " (index.mapping.total_fields.limit)");
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
          // '*' is the wildcard of path_match: a name holding it would make the dynamic templates
          // of typed maps match paths they were not generated for (ADR 0011)
          throw new MappingException(protoPath, "elastic_field.name '" + esName
              + "' must be non-empty and contain no '.', '*' or whitespace");
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
      if (mapValueDepth > 0 && isUntypedValue(field)) {
        return disabledInValues(field, options, path, protoPath);
      }
      if (isLooseValue(field)) {
        String typeName = field.getMessageType().getFullName();
        if (options.getType() == EsType.AUTO) {
          return disabled(field, path, protoPath);
        }
        if (mapValueDepth > 0) {
          throw new MappingException(path, options.getType() + " cannot be applied to " + typeName
              + " inside the values of a typed map, which is mapped as a disabled object; set type:"
              + " IGNORE to leave it out");
        }
        warnings.add(path + ": " + typeName + " mapped as " + options.getType() + " rejects"
            + " documents whose value is not an object (a scalar, an array of scalars or null)");
      }
      FieldKind kind = kindOf(field, options, path);
      if (mapValueDepth > 0) {
        kind = structInValues(field, options, kind, path);
      }
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
          if (field.isMapField()) {
            property.put("dynamic", true);
            typedMap(field, options, path, protoPath);
          } else if (isStructLike(field)) {
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
      boolean typedMap = field.isMapField() && kind.isContainer();
      if (options.getValueType() != EsType.AUTO && !typedMap) {
        warnings.add(path + ": value_type applies only to map fields mapped as OBJECT or NESTED"
            + " and is ignored");
      }
      if (typedMap) {
        // index, doc_values, null_value, copy_to and full_text describe the values
        options = options.toBuilder().clearIndex().clearDocValues().clearNullValue().clearCopyTo()
            .clearFullText().build();
      }

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
          new FieldInfo(path, protoPath, kind, field.isRepeated(), keywordSubfield, boost, field,
              null, !Boolean.FALSE.equals(property.get("enabled"))));
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

    /**
     * Maps an {@code Any}, {@code FieldMask} or {@code Empty} inside the values of a typed map as a
     * disabled object. Outside typed maps such a field is left out; inside them it is kept in
     * {@code _source} unparsed, which also keeps its content from being mapped through the map's
     * dynamic container. The template has no {@code match_mapping_type}, so it also takes the
     * string a {@code FieldMask} prints as.
     */
    private Map<String, Object> disabledInValues(FieldDescriptor field, FieldOpts options,
        String path, String protoPath) {
      String typeName = field.getMessageType().getFullName();
      if (options.getType() != EsType.AUTO) {
        throw new MappingException(path, options.getType() + " cannot be applied to " + typeName
            + " inside the values of a typed map, which is mapped as a disabled object; set type:"
            + " IGNORE to leave it out");
      }
      warnings.add(path + ": " + typeName + " inside the values of a typed map is mapped as a"
          + " disabled object (kept in _source, not indexed)"
          + ("google.protobuf.Any".equals(typeName)
              ? "; the codec writes it only with CodecConfig.anyTypes and leaves it out otherwise"
              : ""));
      return disabled(field, path, protoPath);
    }

    /**
     * Maps a field as {@code {"type": "object", "enabled": false}}: the engine keeps any JSON
     * value in {@code _source} without parsing it. Field options such as {@code boost}, {@code
     * copy_to} or {@code raw_mapping_json} do not apply.
     */
    private Map<String, Object> disabled(FieldDescriptor field, String path, String protoPath) {
      fields.put(path, new FieldInfo(path, protoPath, FieldKind.OBJECT, field.isRepeated(), false,
          1.0f, field, null, false));
      return disabledObject();
    }

    /**
     * Keeps a {@code Struct}, {@code Value} or {@code ListValue} inside the values of a typed map
     * from becoming a dynamic object: the map's templates match with {@code *} across dots, so
     * they would also match the arbitrary keys below it and give them the types of the value
     * message's fields. An explicit {@code OBJECT} or {@code NESTED} is refused; {@link
     * MappingDefaults#structKind()} {@code OBJECT} falls back to {@code FLATTENED} with a warning.
     */
    private FieldKind structInValues(FieldDescriptor field, FieldOpts options, FieldKind kind,
        String path) {
      if (kind == null || !kind.isContainer() || !isStructLike(field)) {
        return kind;
      }
      String typeName = field.getMessageType().getFullName();
      if (options.getType() != EsType.AUTO) {
        throw new MappingException(path, kind + " cannot be applied to " + typeName
            + " inside the values of a typed map: the map's dynamic templates would also match"
            + " its keys; use FLATTENED");
      }
      warnings.add(path + ": " + typeName + " inside the values of a typed map is mapped as"
          + " flattened, not " + kind + " (MappingDefaults.structKind), since the map's dynamic"
          + " templates would also match its keys");
      return FieldKind.FLATTENED;
    }

    /**
     * Maps the values of a map field mapped as {@code OBJECT} or {@code NESTED} to dynamic
     * templates, so that every key takes the value type. The value is mapped by the ordinary rules
     * into scratch collectors with {@code *} for the key; the resulting fields become {@link
     * #dynamicFields} and the resulting properties become templates. A typed map inside the value
     * recurses here with its pattern path ({@code outer.*.inner}), so its own templates carry one
     * {@code *} per key level (ADR 0011).
     */
    private void typedMap(FieldDescriptor field, FieldOpts options, String path,
        String protoPath) {
      FieldDescriptor value = field.getMessageType().findFieldByName("value");
      EsType valueType = options.getValueType();
      if (valueType == EsType.IGNORE) {
        throw new MappingException(path, "value_type IGNORE is not supported; set type: IGNORE to"
            + " leave the map out");
      }
      if (valueType != EsType.AUTO) {
        try {
          validate(FieldKind.fromEsType(valueType), value, path);
        } catch (MappingException e) {
          throw new MappingException(path,
              "value_type " + e.getMessage().substring(path.length() + 2));
        }
      }
      FieldOpts valueOptions = options.toBuilder().setType(valueType).clearValueType().clearName()
          .clearId().clearCopyTo().clearRawMappingJson().clearFullText().build();
      String valuePath = path + ".*";
      String valueProtoPath = protoPath + ".*";
      float boost = options.getBoost() > 0 ? options.getBoost() : 1.0f;
      boolean fullText = options.getFullText() == Bool3.TRUE;

      if (!isMessageValue(value, valueType)) {
        Scratch scratch = scratch(() -> property(value, valueOptions, valuePath, valueProtoPath));
        template(valuePath, false, scratch.mapping());
        scratch.fields().forEach((pattern, info) -> dynamicFields.put(pattern, dynamic(info)));
        FieldKind kind = scratch.fields().get(valuePath).kind();
        if (fullText && (kind.isText() || kind == FieldKind.KEYWORD)) {
          fullTextFields.put(valuePath, boost);
        }
        keyWarning(path, 1 + subFieldCount(scratch.mapping()), 0);
        return;
      }

      Descriptor message = value.getMessageType();
      Scratch scratch = scratch(() -> descend(message, valuePath, valueProtoPath));
      if (scratch.mapping() == null) {
        template(valuePath, false, disabledObject());
        dynamicFields.put(valuePath, new FieldInfo(valuePath, valueProtoPath, FieldKind.OBJECT,
            false, false, boost, value, valuePath, false));
        keyWarning(path, 1, 0);
        return;
      }
      FieldKind rootKind = valueType == EsType.NESTED ? FieldKind.NESTED : FieldKind.OBJECT;
      template(valuePath, true, container(rootKind.esType()));
      dynamicFields.put(valuePath, new FieldInfo(valuePath, valueProtoPath, rootKind, false, false,
          boost, value, valuePath));
      scratch.fields().forEach((pattern, info) -> dynamicFields.put(pattern, dynamic(info)));
      // the object (or nested) field of the key itself, then everything inside the value
      int[] counts = {1, rootKind == FieldKind.NESTED ? 1 : 0};
      templates(scratch.mapping(), valuePath, counts);
      if (fullText && rootKind != FieldKind.NESTED) {
        scratch.fullText().forEach((pattern, fieldBoost) -> {
          boolean underNested = scratch.fields().values().stream()
              .anyMatch(info -> info.kind() == FieldKind.NESTED
                  && pattern.startsWith(info.path() + "."));
          if (!underNested) {
            fullTextFields.put(pattern, fieldBoost);
          }
        });
      }
      keyWarning(path, counts[0], counts[1]);
    }

    /**
     * Turns the properties of a map value into templates, counting the fields they add per key the
     * way the engines count them.
     *
     * @param counts {@code [fields, nested fields]}, incremented in place
     */
    @SuppressWarnings("unchecked")
    private void templates(Map<String, Object> properties, String prefix, int[] counts) {
      for (Map.Entry<String, Object> entry : properties.entrySet()) {
        String pattern = prefix + "." + entry.getKey();
        Map<String, Object> property = (Map<String, Object>) entry.getValue();
        String type = String.valueOf(property.get("type"));
        if (property.get("properties") instanceof Map) {
          template(pattern, true, container(type));
          counts[0]++;
          if (FieldKind.NESTED.esType().equals(type)) {
            counts[1]++;
          }
          templates((Map<String, Object>) property.get("properties"), pattern, counts);
        } else if (Boolean.TRUE.equals(property.get("dynamic"))) {
          // the container of a typed map inside the value; its keys have templates of their own
          template(pattern, true, property);
          counts[0]++;
          if (FieldKind.NESTED.esType().equals(type)) {
            counts[1]++;
          }
        } else {
          template(pattern, false, property);
          counts[0] += 1 + subFieldCount(property);
        }
      }
    }

    private void template(String pathMatch, boolean objectsOnly, Map<String, Object> mapping) {
      Map<String, Object> template = new TreeMap<>();
      template.put("path_match", pathMatch);
      if (objectsOnly) {
        template.put("match_mapping_type", "object");
      }
      template.put("mapping", mapping);
      templates.put("ep:" + pathMatch, template);
    }

    private void keyWarning(String path, int count, int nested) {
      warnings.add("each distinct key of " + path + " adds " + count
          + " field(s) at index time (index.mapping.total_fields.limit)"
          + (nested == 0 ? "" : ", " + nested + " of them nested"
              + " (index.mapping.nested_fields.limit, default 50)"));
    }

    /**
     * Runs a mapping step with empty field, nested-path, full-text and id collectors, so that the
     * values of a typed map do not land among the declared fields.
     */
    private Scratch scratch(Supplier<Map<String, Object>> step) {
      final Map<String, FieldInfo> savedFields = fields;
      final Set<String> savedNested = nestedPaths;
      final Map<String, Float> savedFullText = fullTextFields;
      final String savedId = idField;
      fields = new TreeMap<>();
      nestedPaths = new TreeSet<>();
      fullTextFields = new TreeMap<>();
      idField = null;
      mapValueDepth++;
      try {
        Map<String, Object> mapping = step.get();
        if (idField != null) {
          warnings.add(idField + ": a field inside the values of a map cannot be the document id;"
              + " ignored");
        }
        return new Scratch(mapping, fields, fullTextFields);
      } finally {
        mapValueDepth--;
        fields = savedFields;
        nestedPaths = savedNested;
        fullTextFields = savedFullText;
        idField = savedId;
      }
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
        // EsType.OBJECT on the field opts in to a typed map (dynamic templates from the value).
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
          // Value and ListValue are disabled objects unless a type is requested (see property)
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
      counts[1]++;
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

  /** The mapping, fields and full-text fields of one scratch mapping step. */
  private record Scratch(
      Map<String, Object> mapping, Map<String, FieldInfo> fields, Map<String, Float> fullText) {}

  private static FieldInfo dynamic(FieldInfo info) {
    return new FieldInfo(info.path(), info.protoPath(), info.kind(), info.repeated(),
        info.keywordSubfield(), info.boost(), info.field(), info.path(), info.enabled());
  }

  private static Map<String, Object> container(String type) {
    Map<String, Object> mapping = new TreeMap<>();
    mapping.put("type", type);
    mapping.put("dynamic", true);
    return mapping;
  }

  @SuppressWarnings("unchecked")
  private static int subFieldCount(Map<String, Object> property) {
    Object subFields = property.get("fields");
    return subFields instanceof Map ? ((Map<String, Object>) subFields).size() : 0;
  }

  private static int segments(String templateName) {
    return templateName.split("\\.", -1).length;
  }

  /**
   * Returns whether the value of a typed map is a message whose fields become templates, rather
   * than one value: any message but a wrapper, {@code Struct}-like or {@code Any}-like type, and
   * with {@code value_type} {@code AUTO} also not a {@code Timestamp}, {@code Duration} or geo
   * message.
   */
  private static boolean isMessageValue(FieldDescriptor value, EsType valueType) {
    if (value.getType() != FieldDescriptor.Type.MESSAGE || isWrapper(value)) {
      return false;
    }
    String name = value.getMessageType().getFullName();
    if (STRUCT_LIKE.contains(name) || UNTYPED_VALUES.contains(name)) {
      return false;
    }
    return valueType == EsType.OBJECT || valueType == EsType.NESTED
        || (valueType == EsType.AUTO && !SINGLE_VALUE_MESSAGES.contains(name));
  }

  /**
   * Returns whether a field holds a {@code Value} or {@code ListValue}, which print as any JSON: a
   * scalar, an array, an object or null. {@code flattened} accepts only objects, so they are
   * disabled objects unless a type is requested.
   */
  private static boolean isLooseValue(FieldDescriptor field) {
    return field.getType() == FieldDescriptor.Type.MESSAGE
        && LOOSE_VALUES.contains(field.getMessageType().getFullName());
  }

  /** Returns whether a field holds an {@code Any}, {@code FieldMask} or {@code Empty}. */
  private static boolean isUntypedValue(FieldDescriptor field) {
    return field.getType() == FieldDescriptor.Type.MESSAGE
        && UNTYPED_VALUES.contains(field.getMessageType().getFullName());
  }

  private static Map<String, Object> disabledObject() {
    Map<String, Object> mapping = new TreeMap<>();
    mapping.put("type", FieldKind.OBJECT.esType());
    mapping.put("enabled", false);
    return mapping;
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
