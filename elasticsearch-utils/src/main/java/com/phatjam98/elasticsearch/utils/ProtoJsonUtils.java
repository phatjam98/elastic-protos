package com.phatjam98.elasticsearch.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.phatjam98.core.common.proto.GeoBufProtos;
import com.phatjam98.geobuf.utils.GeobufUtils;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProtoJsonUtils is used to convert a protobuf message to a JSON string. This is used to convert
 * geobuf data to GeoJSON for use in Elasticsearch.
 */
public class ProtoJsonUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoJsonUtils.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Converts a protobuf message to a JSON string including GeoJSON from GeoBuf.Data.
   *
   * @param proto protobuf message
   * @return JSON string
   */
  public static String getJsonFromProto(GeneratedMessageV3 proto) {
    try {
      Map<String, Object> jsonMap = convertProtoToJsonMap(proto);
      Map<String, Object> geoJsonMap = getGeoJsonMap(proto);
      jsonMap = mergeMaps(jsonMap, geoJsonMap);
      return MAPPER.writeValueAsString(jsonMap);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Converts a protobuf message to a Map.
   *
   * @param proto protobuf message
   * @return Map
   * @throws InvalidProtocolBufferException protobuf message invalid
   * @throws JsonProcessingException        JSON processing error
   */
  public static Map<String, Object> convertProtoToJsonMap(GeneratedMessageV3 proto)
      throws InvalidProtocolBufferException, JsonProcessingException {
    String protoJson = JsonFormat.printer().omittingInsignificantWhitespace()
        .preservingProtoFieldNames().print(proto);
    return MAPPER.readValue(protoJson, new TypeReference<Map<String, Object>>() {
    });
  }

  /**
   * Converts a JSON string to a Map.
   *
   * @param jsonStr JSON string
   * @return Map
   * @throws JsonProcessingException JSON processing error
   */
  public static Map<String, Object> convertStringToMap(String jsonStr)
      throws JsonProcessingException {
    return MAPPER.readValue(jsonStr, new TypeReference<Map<String, Object>>() {
    });
  }

  /**
   * Converts a JSON string to a protobuf message with GeoBufProtos.Data converted from GeoJSON.
   *
   * @param jsonStr JSON string
   * @param klass   protobuf message class
   * @param <T>     protobuf message type
   * @return protobuf message
   */
  public static <T extends GeneratedMessageV3> T convertJsonToProto(String jsonStr,
                                                                    Class<T> klass) {
    GeneratedMessageV3.Builder builder = null;
    try {
      Method newBuilderMethod = klass.getDeclaredMethod("newBuilder");
      builder = (GeneratedMessageV3.Builder) newBuilderMethod.invoke(null);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("No newBuilder method found", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Cannot access newBuilder method", e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("Error invoking newBuilder method", e);
    }

    return convertJsonToProto(jsonStr, builder);
  }

  public static <T extends GeneratedMessageV3> T convertJsonToProto(String jsonStr,
                                                              GeneratedMessageV3.Builder builder) {
    try {
      Map<String, Object> jsonMap = convertStringToMap(jsonStr);
      convertGeoJsonToGeoBufInMap(jsonMap);
      convertTimeStringToTimestampInMap(jsonMap);
      String modifiedJsonStr = MAPPER.writeValueAsString(jsonMap);
      JsonFormat.parser().ignoringUnknownFields().merge(modifiedJsonStr, builder);
      return (T) builder.build();
    } catch (InvalidProtocolBufferException | JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static void convertTimeStringToTimestampInMap(Map<String, Object> jsonMap) {
    for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Map) {
        convertTimeStringToTimestampInMap((Map<String, Object>) value);
      } else if (value instanceof List) {
        handleListEntry((List<Object>) value);
      } else if (value instanceof String && entry.getKey().endsWith("_at")) {
        String strValue = (String) value;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        Date date = null;

        try {
          date = formatter.parse(strValue);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }

        if (date != null) {
          var instant = date.toInstant();
          var zonedDateTime = instant.atZone(ZoneId.of("UTC"));
          var timeString = DateTimeFormatter.ISO_INSTANT.format(zonedDateTime);
          jsonMap.put(entry.getKey(), timeString);
        }
      }
    }
  }

  private static void handleListEntryForTimestamp(List<Object> valueList) {
    for (Object item : valueList) {
      if (item instanceof Map) {
        convertTimeStringToTimestampInMap((Map<String, Object>) item);
      } else if (item instanceof List) {
        handleListEntryForTimestamp((List<Object>) item);
      }
    }
  }

  private static void convertGeoJsonToGeoBufInMap(Map<String, Object> jsonMap) {
    for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Map) {
        handleMapEntry(entry, (Map<String, Object>) value);
      } else if (value instanceof List) {
        handleListEntry((List<Object>) value);
      }
    }
  }

  private static void handleMapEntry(Map.Entry<String, Object> entry,
                                     Map<String, Object> valueMap) {
    if (isGeoJson(valueMap)) {
      entry.setValue(convertToGeobuf(valueMap));
    } else {
      convertGeoJsonToGeoBufInMap(valueMap);
    }
  }

  private static void handleListEntry(List<Object> valueList) {
    for (Object item : valueList) {
      if (item instanceof Map) {
        convertGeoJsonToGeoBufInMap((Map<String, Object>) item);
      }
    }
  }

  private static Map<String, Object> convertToGeobuf(Map<String, Object> geoJsonMap) {
    try {
      GeobufUtils geoBufUtils = new GeobufUtils(MAPPER.writeValueAsString(geoJsonMap));
      GeoBufProtos.Data geobuf = GeoBufProtos.Data.parseFrom(geoBufUtils.getGeobuf());
      return ProtoJsonUtils.convertProtoToJsonMap(geobuf);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isGeoJson(Map<String, Object> map) {
    // Basic check for GeoJSON by looking for the "type" and "coordinates" fields
    return map.containsKey("type") && map.containsKey("coordinates");
  }

  private static Map<String, Object> getGeoJsonMap(GeneratedMessageV3 proto) {
    Map<String, Object> jsonMap = new HashMap<>();
    var descriptor = proto.getDescriptorForType();

    for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
      if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
        if (field.getMessageType() == GeoBufProtos.Data.getDescriptor()) {
          GeoBufProtos.Data data = (GeoBufProtos.Data) proto.getField(field);
          var geoBuf = new GeobufUtils(data);
          jsonMap.put(field.getName(), geoBuf.getGeoJson());
        } else if (field.isRepeated()) {
          LOGGER.debug("Repeated field geospatial conversion is not yet implemented for field: {}",
              field.getName());
        } else {
          Map<String, Object> childJsonMap =
              getGeoJsonMap((GeneratedMessageV3) proto.getField(field));
          if (!childJsonMap.isEmpty()) {
            jsonMap.put(field.getName(), childJsonMap);
          }
        }
      }
    }

    return jsonMap;
  }

  private static Map<String, Object> mergeMaps(Map<String, Object> protoJson,
                                               Map<String, Object> updateJson) {
    for (Map.Entry<String, Object> entry : protoJson.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();

      if (updateJson.containsKey(key)) {
        if (value instanceof Map && updateJson.get(key) instanceof Map) {
          Map<String, Object> childMap =
              mergeMaps((Map<String, Object>) value, (Map<String, Object>) updateJson.get(key));
          protoJson.put(key, childMap);
        } else if (value instanceof List && updateJson.get(key) instanceof List) {
          mergeLists((List<Object>) value, (List<Object>) updateJson.get(key));
        } else {
          try {
            protoJson.put(key, convertStringToMap(updateJson.get(key).toString()));
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    return protoJson;
  }

  private static void mergeLists(List<Object> protoList, List<Object> updateList) {
    for (Object updateItem : updateList) {
      if (!protoList.contains(updateItem)) {
        protoList.add(updateItem);
      }
    }
  }
}