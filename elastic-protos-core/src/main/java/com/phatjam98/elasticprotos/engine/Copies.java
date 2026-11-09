package com.phatjam98.elasticprotos.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shallow defensive copies for the result types. Unlike {@code List.copyOf} and {@code
 * Map.copyOf} they accept null elements and values, which engine responses contain, and they keep
 * the iteration order. Only the top level is copied: nested maps and lists are shared.
 */
final class Copies {

  private Copies() {}

  /** Returns an unmodifiable copy of {@code list}, or null when it is null. */
  static <T> List<T> list(List<? extends T> list) {
    return list == null ? null : Collections.unmodifiableList(new ArrayList<>(list));
  }

  /** Returns an unmodifiable copy of {@code map} in its iteration order, or null when null. */
  static <K, V> Map<K, V> map(Map<? extends K, ? extends V> map) {
    return map == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(map));
  }
}
