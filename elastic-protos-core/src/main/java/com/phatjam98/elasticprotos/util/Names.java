package com.phatjam98.elasticprotos.util;

import java.util.Locale;

/** Naming helpers. */
public final class Names {

  private Names() {}

  /**
   * Converts an UpperCamelCase or lowerCamelCase name to snake_case, keeping acronym runs
   * together: {@code InboxItem} becomes {@code inbox_item} and {@code URLMapping} becomes
   * {@code url_mapping}.
   *
   * @param name a camel-case identifier
   * @return the snake_case form
   */
  public static String snakeCase(String name) {
    return name.replaceAll("([a-z0-9])([A-Z])", "$1_$2")
        .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
        .toLowerCase(Locale.ROOT);
  }
}
