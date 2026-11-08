package com.phatjam98.elasticprotos.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * JSON helpers shared across the library: parsing into plain maps, canonical (sorted-key)
 * serialization, hashing and deep merging.
 */
public final class Json {

  private static final JsonMapper MAPPER =
      JsonMapper.builder()
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
          .build();

  private static final TypeReference<Map<String, Object>> OBJECT_TYPE = new TypeReference<>() {};

  private Json() {}

  /**
   * Parses a JSON object into a map whose nested maps are all sorted by key.
   *
   * @param json a JSON object
   * @return a mutable, key-sorted map
   * @throws IllegalArgumentException if the text is not a JSON object
   */
  public static Map<String, Object> parseObject(String json) {
    try {
      return sortedCopy(MAPPER.readValue(json, OBJECT_TYPE));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Not a JSON object: " + json, e);
    }
  }

  /**
   * Parses any JSON value.
   *
   * @param json JSON text
   * @return a map, list, string, number, boolean or null
   * @throws IllegalArgumentException if the text is not valid JSON
   */
  public static Object parse(String json) {
    try {
      return sortedCopy(MAPPER.readValue(json, Object.class));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid JSON: " + json, e);
    }
  }

  /**
   * Serializes a value compactly with map keys in sorted order, so equal trees produce equal text.
   *
   * @param value a map, list, scalar or Jackson-serializable object
   * @return compact JSON
   */
  public static String canonical(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Value cannot be serialized to JSON", e);
    }
  }

  /**
   * Serializes a value with indentation and sorted map keys.
   *
   * @param value a map, list, scalar or Jackson-serializable object
   * @return pretty-printed JSON
   */
  public static String pretty(Object value) {
    try {
      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Value cannot be serialized to JSON", e);
    }
  }

  /**
   * Returns the lower-case hex SHA-256 digest of the canonical JSON form of a value.
   *
   * @param value the value to hash
   * @return 64 hex characters
   */
  public static String sha256Hex(Object value) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256")
              .digest(canonical(value).getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by the JVM specification", e);
    }
  }

  /**
   * Merges {@code overlay} into a copy of {@code base}. Nested maps are merged recursively; any
   * other value in the overlay replaces the base value.
   *
   * @param base the map to start from
   * @param overlay the values that win
   * @return a new key-sorted map
   */
  public static Map<String, Object> deepMerge(
      Map<String, Object> base, Map<String, Object> overlay) {
    Map<String, Object> merged = sortedCopy(base);
    for (Map.Entry<String, Object> entry : overlay.entrySet()) {
      Object existing = merged.get(entry.getKey());
      Object incoming = entry.getValue();
      if (existing instanceof Map && incoming instanceof Map) {
        merged.put(entry.getKey(), deepMerge(asMap(existing), asMap(incoming)));
      } else {
        merged.put(entry.getKey(), sortedCopy(incoming));
      }
    }
    return merged;
  }

  /**
   * Deep-copies a JSON-like tree, turning every map into a {@link TreeMap}.
   *
   * @param value a map, list or scalar
   * @return the copy
   */
  public static Map<String, Object> sortedCopy(Map<String, Object> value) {
    Map<String, Object> copy = new TreeMap<>();
    for (Map.Entry<String, Object> entry : value.entrySet()) {
      copy.put(entry.getKey(), sortedCopy(entry.getValue()));
    }
    return copy;
  }

  @SuppressWarnings("unchecked")
  private static Object sortedCopy(Object value) {
    if (value instanceof Map) {
      return sortedCopy((Map<String, Object>) value);
    }
    if (value instanceof List) {
      List<Object> copy = new ArrayList<>();
      for (Object item : (List<Object>) value) {
        copy.add(sortedCopy(item));
      }
      return copy;
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asMap(Object value) {
    return (Map<String, Object>) value;
  }
}
