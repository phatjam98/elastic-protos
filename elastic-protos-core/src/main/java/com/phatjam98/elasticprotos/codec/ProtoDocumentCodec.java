package com.phatjam98.elasticprotos.codec;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.phatjam98.elasticprotos.engine.Hit;
import com.phatjam98.elasticprotos.mapping.FieldInfo;
import com.phatjam98.elasticprotos.mapping.FieldKind;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.mapping.ProtoMapper;
import com.phatjam98.elasticprotos.util.Json;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Converts messages to index documents and documents back to messages, following the decisions
 * recorded in the message's {@link IndexDefinition}: ignored fields are dropped, renamed fields
 * are renamed, geo values take the shape the engine expects and 64-bit integers are numbers.
 *
 * <p>Everything else follows protobuf's canonical JSON mapping with proto field names: timestamps
 * are RFC 3339 strings, enums are names, bytes are base64, wrappers are unwrapped and
 * {@code Struct} values are plain objects.
 *
 * <p>With {@link CodecConfig#includeDefaults()} (the default) proto3 fields without presence are
 * written even when they hold their default value: {@code false}, {@code 0}, {@code ""}, the first
 * enum value, {@code []} and {@code {}}. Fields with presence are written only when set: an unset
 * sub-message, {@code Timestamp}, wrapper, {@code Struct}, proto3 {@code optional} field or oneof
 * member stays absent. An empty string is written only to keyword and text fields; date, ip, geo,
 * numeric and binary fields leave it out because the engine would reject it or it holds nothing.
 */
public final class ProtoDocumentCodec {

  private static final String TIMESTAMP = "google.protobuf.Timestamp";
  private static final Set<String> LAT_NAMES = Set.of("lat", "latitude");
  private static final Set<String> LON_NAMES = Set.of("lon", "lng", "longitude");
  private static final Set<String> STRUCT_LIKE =
      Set.of("google.protobuf.Struct", "google.protobuf.Value", "google.protobuf.ListValue");

  private final ProtoMapper mapper;
  private final CodecConfig config;
  private final ConcurrentMap<String, Schema> schemas = new ConcurrentHashMap<>();

  private ProtoDocumentCodec(ProtoMapper mapper, CodecConfig config) {
    this.mapper = mapper;
    this.config = config;
  }

  /** Creates a codec that maps messages with the given mapper and standard settings. */
  public static ProtoDocumentCodec of(ProtoMapper mapper) {
    return of(mapper, CodecConfig.standard());
  }

  /** Creates a codec that maps messages with the given mapper and settings. */
  public static ProtoDocumentCodec of(ProtoMapper mapper, CodecConfig config) {
    return new ProtoDocumentCodec(mapper, config);
  }

  /** Returns the settings in use. */
  public CodecConfig config() {
    return config;
  }

  /**
   * Returns the cached index definition for a message type.
   *
   * @param descriptor the message
   * @return the definition the codec works from
   */
  public IndexDefinition definition(Descriptor descriptor) {
    return schema(descriptor).definition;
  }

  /**
   * Converts a message to a document.
   *
   * @param message the message
   * @return a key-sorted map ready to be serialized as the document body
   */
  public Map<String, Object> toDocument(Message message) {
    return toDocument(message, schema(message.getDescriptorForType()));
  }

  /**
   * Converts a message to a document using an explicit definition.
   *
   * @param message the message
   * @param definition the definition to follow
   * @return a key-sorted map ready to be serialized as the document body
   */
  public Map<String, Object> toDocument(Message message, IndexDefinition definition) {
    return toDocument(message, new Schema(definition));
  }

  private Map<String, Object> toDocument(Message message, Schema schema) {
    Message pruned = prune(message, schema.definition.ignoredPaths());
    JsonFormat.Printer printer =
        JsonFormat.printer()
            .preservingProtoFieldNames()
            .omittingInsignificantWhitespace()
            .usingTypeRegistry(config.anyTypes());
    if (config.includeDefaults()) {
      printer = printer.includingDefaultValueFields();
    }
    String json;
    try {
      json = printer.print(pruned);
    } catch (InvalidProtocolBufferException e) {
      throw new CodecException("Cannot serialize " + message.getDescriptorForType().getFullName(),
          e);
    }
    return forward(Json.parseObject(json), message.getDescriptorForType(), "", schema);
  }

  /**
   * Converts a message to a JSON document.
   *
   * @param message the message
   * @return compact JSON
   */
  public String toJson(Message message) {
    return Json.canonical(toDocument(message));
  }

  /**
   * Reads a document back into a generated message type.
   *
   * @param source the document body
   * @param type the generated message class
   * @param <T> the message type
   * @return the message
   */
  public <T extends Message> T fromDocument(Map<String, Object> source, Class<T> type) {
    Descriptor descriptor = ProtoMapper.descriptorOf(type);
    Message.Builder builder;
    try {
      builder = (Message.Builder) type.getMethod("newBuilder").invoke(null);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException(type.getName() + " has no newBuilder()", e);
    }
    return type.cast(fromDocument(source, descriptor, builder));
  }

  /**
   * Reads a document back into a {@link DynamicMessage}.
   *
   * @param source the document body
   * @param descriptor the message type
   * @return the message
   */
  public Message fromDocument(Map<String, Object> source, Descriptor descriptor) {
    return fromDocument(source, descriptor, DynamicMessage.newBuilder(descriptor));
  }

  private Message fromDocument(
      Map<String, Object> source, Descriptor descriptor, Message.Builder builder) {
    Schema schema = schema(descriptor);
    Map<String, Object> protoShaped = reverse(source, descriptor, "", schema);
    JsonFormat.Parser parser = JsonFormat.parser().usingTypeRegistry(config.anyTypes());
    if (config.unknownFields() == UnknownFieldPolicy.IGNORE) {
      parser = parser.ignoringUnknownFields();
    }
    try {
      parser.merge(Json.canonical(protoShaped), builder);
    } catch (InvalidProtocolBufferException e) {
      throw new CodecException("Cannot read document into " + descriptor.getFullName() + ": "
          + e.getMessage(), e);
    }
    return builder.build();
  }

  /**
   * Reads a search hit back into a generated message type.
   *
   * @param hit the hit
   * @param type the generated message class
   * @param <T> the message type
   * @return the message
   */
  public <T extends Message> T fromHit(Hit hit, Class<T> type) {
    return fromDocument(hit.source(), type);
  }

  /**
   * Returns the document id of a message, taken from the field marked {@code id} or named by
   * {@code id_field}.
   *
   * @param message the message
   * @return the id, or empty when the message type declares no id field or the value is unset
   * @throws CodecException if the id field holds a value that cannot be an id
   */
  public Optional<String> idOf(Message message) {
    Schema schema = schema(message.getDescriptorForType());
    if (schema.definition.idField() == null) {
      return Optional.empty();
    }
    FieldInfo info = schema.definition.fields().get(schema.definition.idField());
    String[] segments = info.protoPath().split("\\.");
    Message current = message;
    for (int i = 0; i < segments.length - 1; i++) {
      FieldDescriptor field = current.getDescriptorForType().findFieldByName(segments[i]);
      if (field.isRepeated() || !current.hasField(field)) {
        return Optional.empty();
      }
      current = (Message) current.getField(field);
    }
    FieldDescriptor field = current.getDescriptorForType().findFieldByName(segments[segments.length
        - 1]);
    if (field.isRepeated()) {
      throw new CodecException("id field " + info.protoPath() + " is repeated");
    }
    return idValue(current.getField(field), info.protoPath());
  }

  private static Optional<String> idValue(Object value, String path) {
    if (value instanceof String) {
      return ((String) value).isEmpty() ? Optional.empty() : Optional.of((String) value);
    }
    if (value instanceof Number || value instanceof Boolean) {
      return Optional.of(String.valueOf(value));
    }
    if (value instanceof EnumValueDescriptor) {
      return Optional.of(((EnumValueDescriptor) value).getName());
    }
    if (value instanceof Timestamp) {
      return Optional.of(Timestamps.toString((Timestamp) value));
    }
    throw new CodecException("id field " + path + " holds a " + value.getClass().getSimpleName()
        + ", which cannot be a document id");
  }

  private Map<String, Object> forward(
      Map<String, Object> in, Descriptor descriptor, String prefix, Schema schema) {
    Map<String, Object> out = new TreeMap<>();
    for (FieldDescriptor field : descriptor.getFields()) {
      Object value = in.get(field.getName());
      String protoPath = join(prefix, field.getName());
      FieldInfo info = schema.byProtoPath.get(protoPath);
      if (value == null || info == null) {
        continue;
      }
      Object converted = convertEach(value, field, item -> forwardValue(item, field, info, schema));
      if (converted != null) {
        out.put(info.esName(), converted);
      }
    }
    return out;
  }

  private Object forwardValue(Object value, FieldDescriptor field, FieldInfo info, Schema schema) {
    if ("".equals(value) && !keepsEmptyString(info.kind())) {
      return null;
    }
    switch (info.kind()) {
      case LONG:
      case INTEGER:
        if (config.int64AsNumber() && value instanceof String) {
          try {
            return Long.valueOf((String) value);
          } catch (NumberFormatException e) {
            throw new CodecException("Field " + info.protoPath() + " holds " + value
                + ", which does not fit the " + info.kind().esType() + " it is mapped as", e);
          }
        }
        return value;
      case GEO_POINT:
        return value instanceof Map ? geoPoint(asMap(value)) : value;
      case GEO_SHAPE:
        if (value instanceof Map) {
          Map<String, Object> shape = asMap(value);
          Object wkt = shape.get("wkt");
          Object geojson = shape.get("geojson");
          if (wkt instanceof String && !((String) wkt).isEmpty()) {
            return wkt;
          }
          return geojson instanceof String && !((String) geojson).isEmpty()
              ? Json.parse((String) geojson)
              : null;
        }
        return value instanceof String && ((String) value).startsWith("{")
            ? Json.parse((String) value)
            : value;
      case OBJECT:
      case NESTED:
        if (value instanceof Map && schema.hasChildren(info.protoPath()) && !isOpaque(field)) {
          return forward(asMap(value), field.getMessageType(), info.protoPath(), schema);
        }
        return value;
      default:
        return value;
    }
  }

  private Map<String, Object> reverse(
      Map<String, Object> in, Descriptor descriptor, String prefix, Schema schema) {
    Map<String, Object> out = new TreeMap<>();
    Set<String> known = new HashSet<>();
    for (FieldDescriptor field : descriptor.getFields()) {
      String protoPath = join(prefix, field.getName());
      FieldInfo info = schema.byProtoPath.get(protoPath);
      if (info == null) {
        continue;
      }
      known.add(info.esName());
      Object value = in.get(info.esName());
      if (value == null) {
        continue;
      }
      Object converted = convertEach(value, field, item -> reverseValue(item, field, info, schema));
      if (converted != null) {
        out.put(field.getName(), converted);
      }
    }
    if (config.unknownFields() == UnknownFieldPolicy.FAIL) {
      for (String key : in.keySet()) {
        if (!known.contains(key)) {
          throw new CodecException("Unknown document field " + join(prefix, key) + " for "
              + descriptor.getFullName());
        }
      }
    }
    return out;
  }

  private Object reverseValue(Object value, FieldDescriptor field, FieldInfo info, Schema schema) {
    boolean message = field.getJavaType() == FieldDescriptor.JavaType.MESSAGE;
    switch (info.kind()) {
      case DATE:
      case DATE_NANOS:
        if (message && value instanceof Number) {
          return Instant.ofEpochMilli(((Number) value).longValue()).toString();
        }
        return value;
      case GEO_POINT:
        return message ? latLonForMessage(value, field.getMessageType()) : latLonString(value);
      case GEO_SHAPE:
        if (message) {
          Map<String, Object> shape = new TreeMap<>();
          shape.put(value instanceof String ? "wkt" : "geojson",
              value instanceof String ? value : Json.canonical(value));
          return shape;
        }
        return value instanceof String ? value : Json.canonical(value);
      case OBJECT:
      case NESTED:
        if (value instanceof Map && schema.hasChildren(info.protoPath()) && !isOpaque(field)) {
          return reverse(asMap(value), field.getMessageType(), info.protoPath(), schema);
        }
        return value;
      default:
        return value;
    }
  }

  /**
   * Returns whether an empty string is a value worth writing for a kind. Only keyword and text
   * fields index {@code ""}; date, ip, geo and numeric fields would reject the document, and an
   * empty binary value carries nothing, so a proto3 default {@code ""} is left out for them.
   */
  private static boolean keepsEmptyString(FieldKind kind) {
    return kind == FieldKind.KEYWORD || kind.isText();
  }

  private static Object convertEach(
      Object value, FieldDescriptor field, java.util.function.Function<Object, Object> convert) {
    if (field.isRepeated() && !field.isMapField() && value instanceof List) {
      List<Object> out = new ArrayList<>();
      for (Object item : (List<?>) value) {
        Object converted = convert.apply(item);
        if (converted != null) {
          out.add(converted);
        }
      }
      return out;
    }
    return convert.apply(value);
  }

  /**
   * Converts a printed lat/lon message to {@code {"lat": .., "lon": ..}}. Canonical JSON leaves out
   * coordinates that are 0, and engines reject a point without both, so missing ones are 0.0.
   */
  private static Map<String, Object> geoPoint(Map<String, Object> in) {
    Map<String, Object> out = latLon(in);
    out.putIfAbsent("lat", 0.0d);
    out.putIfAbsent("lon", 0.0d);
    return out;
  }

  /** Normalizes a lat/lon-like object to the {@code {"lat": .., "lon": ..}} form engines accept. */
  private static Map<String, Object> latLon(Map<String, Object> in) {
    Map<String, Object> out = new TreeMap<>();
    for (Map.Entry<String, Object> entry : in.entrySet()) {
      if (LAT_NAMES.contains(entry.getKey())) {
        out.put("lat", entry.getValue());
      } else if (LON_NAMES.contains(entry.getKey())) {
        out.put("lon", entry.getValue());
      }
    }
    return out;
  }

  private static Map<String, Object> latLonForMessage(Object value, Descriptor target) {
    double[] point = parsePoint(value);
    Map<String, Object> out = new TreeMap<>();
    for (FieldDescriptor field : target.getFields()) {
      if (LAT_NAMES.contains(field.getName())) {
        out.put(field.getName(), point[0]);
      } else if (LON_NAMES.contains(field.getName())) {
        out.put(field.getName(), point[1]);
      }
    }
    return out;
  }

  private static Object latLonString(Object value) {
    if (value instanceof String) {
      return value;
    }
    double[] point = parsePoint(value);
    return point[0] + "," + point[1];
  }

  /**
   * Accepts {@code {lat, lon}}, {@code [lon, lat]}, {@code "lat,lon"} and {@code POINT (lon lat)}.
   */
  private static double[] parsePoint(Object value) {
    if (value instanceof Map) {
      Map<String, Object> normalized = latLon(asMap(value));
      return new double[] {number(normalized.get("lat")), number(normalized.get("lon"))};
    }
    if (value instanceof List && ((List<?>) value).size() == 2) {
      List<?> list = (List<?>) value;
      return new double[] {number(list.get(1)), number(list.get(0))};
    }
    if (value instanceof String) {
      String text = ((String) value).trim();
      if (text.regionMatches(true, 0, "POINT", 0, 5)) {
        String[] parts = text.substring(text.indexOf('(') + 1, text.indexOf(')')).trim()
            .split("\\s+");
        return new double[] {Double.parseDouble(parts[1]), Double.parseDouble(parts[0])};
      }
      String[] parts = text.split(",");
      return new double[] {Double.parseDouble(parts[0].trim()), Double.parseDouble(parts[1]
          .trim())};
    }
    throw new CodecException("Not a geo point: " + value);
  }

  private static double number(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    throw new CodecException("Not a coordinate: " + value);
  }

  private static boolean isOpaque(FieldDescriptor field) {
    return field.isMapField() || STRUCT_LIKE.contains(field.getMessageType().getFullName());
  }

  private static Message prune(Message message, Set<String> ignored) {
    if (ignored.isEmpty()) {
      return message;
    }
    Message.Builder builder = message.toBuilder();
    prune(builder, "", ignored);
    return builder.build();
  }

  private static void prune(Message.Builder builder, String prefix, Set<String> ignored) {
    for (FieldDescriptor field : builder.getDescriptorForType().getFields()) {
      String path = join(prefix, field.getName());
      if (ignored.contains(path)) {
        builder.clearField(field);
        continue;
      }
      if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE || field.isMapField()
          || ignored.stream().noneMatch(p -> p.startsWith(path + "."))) {
        continue;
      }
      if (field.isRepeated()) {
        for (int i = 0; i < builder.getRepeatedFieldCount(field); i++) {
          Message.Builder child = ((Message) builder.getRepeatedField(field, i)).toBuilder();
          prune(child, path, ignored);
          builder.setRepeatedField(field, i, child.build());
        }
      } else if (builder.hasField(field)) {
        Message.Builder child = ((Message) builder.getField(field)).toBuilder();
        prune(child, path, ignored);
        builder.setField(field, child.build());
      }
    }
  }

  private Schema schema(Descriptor descriptor) {
    return schemas.computeIfAbsent(descriptor.getFullName(),
        name -> new Schema(mapper.map(descriptor)));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asMap(Object value) {
    return (Map<String, Object>) value;
  }

  private static String join(String prefix, String name) {
    return prefix.isEmpty() ? name : prefix + "." + name;
  }

  /** An index definition indexed by proto path. */
  private static final class Schema {

    private final IndexDefinition definition;
    private final Map<String, FieldInfo> byProtoPath = new HashMap<>();
    private final Set<String> parents = new HashSet<>();

    Schema(IndexDefinition definition) {
      this.definition = definition;
      for (FieldInfo info : definition.fields().values()) {
        byProtoPath.put(info.protoPath(), info);
        int dot = info.protoPath().lastIndexOf('.');
        if (dot > 0) {
          parents.add(info.protoPath().substring(0, dot));
        }
      }
    }

    boolean hasChildren(String protoPath) {
      return parents.contains(protoPath);
    }
  }
}
