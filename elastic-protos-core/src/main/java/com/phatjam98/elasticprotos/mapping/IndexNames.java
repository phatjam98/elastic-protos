package com.phatjam98.elasticprotos.mapping;

import java.util.Locale;
import java.util.Objects;

/** Checks the parts of alias and index names: prefixes and declared names. */
final class IndexNames {

  private static final String INVALID = "\\/*?\"<>|,#:";

  private IndexNames() {}

  /**
   * Lower-cases a name part and checks that it can start an index name: it must not be empty,
   * must not start with {@code -}, {@code _}, {@code +} or {@code .}, and must not contain
   * whitespace or any of {@code \ / * ? " < > | , # :}.
   *
   * @param what what the value is, for the message (for example {@code index name prefix})
   * @param value the value
   * @return the lower-cased value
   * @throws IllegalArgumentException if the value cannot start an index name
   */
  static String check(String what, String value) {
    Objects.requireNonNull(value, what);
    String lower = value.toLowerCase(Locale.ROOT);
    if (lower.isEmpty()) {
      throw new IllegalArgumentException(what + " must not be empty");
    }
    char first = lower.charAt(0);
    if (first == '-' || first == '_' || first == '+' || first == '.') {
      throw new IllegalArgumentException(
          what + " must not start with '" + first + "': " + value);
    }
    for (int i = 0; i < lower.length(); i++) {
      char c = lower.charAt(i);
      if (Character.isWhitespace(c) || INVALID.indexOf(c) >= 0) {
        throw new IllegalArgumentException(what + " must not contain '" + c + "': " + value);
      }
    }
    return lower;
  }
}
