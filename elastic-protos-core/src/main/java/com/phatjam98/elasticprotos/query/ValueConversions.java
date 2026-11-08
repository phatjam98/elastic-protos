package com.phatjam98.elasticprotos.query;

import com.google.protobuf.util.Timestamps;
import com.phatjam98.elasticprotos.mapping.FieldInfo;
import com.phatjam98.elasticprotos.mapping.FieldKind;
import com.phatjam98.elasticprotos.proto.FlatValue;
import com.phatjam98.elasticprotos.proto.PrimitiveValue;
import java.util.Locale;

/** Converts operand values to the JSON type a field's kind expects. */
final class ValueConversions {

  private ValueConversions() {}

  /** Returns whether a kind holds plain strings that term-level queries compare exactly. */
  static boolean isStringLike(FieldKind kind) {
    return kind == FieldKind.KEYWORD
        || kind.isText()
        || kind == FieldKind.IP
        || kind == FieldKind.BINARY
        || kind == FieldKind.FLATTENED;
  }

  /**
   * Converts a scalar for term-level queries ({@code term}, {@code terms}).
   *
   * @throws QueryCompileException when the value does not fit the field
   */
  static Object scalar(FieldInfo field, FlatValue value) {
    FieldKind kind = field.kind();
    if (kind.isNumeric()) {
      return numeric(field, value);
    }
    if (kind == FieldKind.BOOLEAN) {
      return bool(field, value);
    }
    if (kind.isDate()) {
      return date(field, value);
    }
    if (isStringLike(kind)) {
      return string(field, value);
    }
    throw mismatch(field, value);
  }

  /** Converts a value to a string for text queries; strings, numbers and booleans qualify. */
  static String string(FieldInfo field, FlatValue value) {
    switch (value.getKindCase()) {
      case STRING_VALUE:
        return value.getStringValue();
      case NUMBER_VALUE:
        return numberText(value.getNumberValue());
      case INT_VALUE:
        return Long.toString(value.getIntValue());
      case BOOL_VALUE:
        return Boolean.toString(value.getBoolValue());
      default:
        throw mismatch(field, value);
    }
  }

  /** Returns a range bound typed for the field: a Long when integral and the kind allows it. */
  static Object number(FieldKind kind, double number) {
    boolean integralKind = kind == FieldKind.INTEGER || kind == FieldKind.LONG || kind.isDate();
    if (integralKind && isIntegral(number)) {
      return (long) number;
    }
    return number;
  }

  private static Object numeric(FieldInfo field, FlatValue value) {
    FieldKind kind = field.kind();
    switch (value.getKindCase()) {
      case NUMBER_VALUE:
        return checkFinite(field, value, value.getNumberValue());
      case INT_VALUE:
        if (kind == FieldKind.INTEGER || kind == FieldKind.LONG) {
          return value.getIntValue();
        }
        return (double) value.getIntValue();
      case STRING_VALUE:
        String text = value.getStringValue().trim();
        if (kind == FieldKind.INTEGER || kind == FieldKind.LONG) {
          try {
            return Long.parseLong(text);
          } catch (NumberFormatException e) {
            // fall through to the double form, e.g. "1.5" or "1e3"
          }
        }
        try {
          return checkFinite(field, value, Double.parseDouble(text));
        } catch (NumberFormatException e) {
          throw mismatch(field, value);
        }
      default:
        throw mismatch(field, value);
    }
  }

  private static Object checkFinite(FieldInfo field, FlatValue value, double number) {
    if (Double.isNaN(number) || Double.isInfinite(number)) {
      throw mismatch(field, value);
    }
    return number(field.kind(), number);
  }

  private static Boolean bool(FieldInfo field, FlatValue value) {
    if (value.getKindCase() == FlatValue.KindCase.BOOL_VALUE) {
      return value.getBoolValue();
    }
    if (value.getKindCase() == FlatValue.KindCase.STRING_VALUE) {
      String text = value.getStringValue().trim().toLowerCase(Locale.ROOT);
      if (text.equals("true") || text.equals("false")) {
        return Boolean.valueOf(text);
      }
    }
    throw mismatch(field, value);
  }

  private static String date(FieldInfo field, FlatValue value) {
    try {
      switch (value.getKindCase()) {
        case TIMESTAMP_VALUE:
          return Timestamps.toString(value.getTimestampValue());
        case STRING_VALUE:
          return value.getStringValue();
        case INT_VALUE:
          return Timestamps.toString(Timestamps.fromMillis(value.getIntValue()));
        case NUMBER_VALUE:
          return Timestamps.toString(Timestamps.fromMillis((long) value.getNumberValue()));
        default:
          throw mismatch(field, value);
      }
    } catch (IllegalArgumentException e) {
      throw new QueryCompileException(
          "Field '" + field.path() + "' got a timestamp outside the supported range: "
              + e.getMessage(),
          field.path());
    }
  }

  /** Copies a list item into a FlatValue so the scalar rules apply to it. */
  static FlatValue of(PrimitiveValue value) {
    switch (value.getKindCase()) {
      case NUMBER_VALUE:
        return FlatValue.newBuilder().setNumberValue(value.getNumberValue()).build();
      case STRING_VALUE:
        return FlatValue.newBuilder().setStringValue(value.getStringValue()).build();
      case BOOL_VALUE:
        return FlatValue.newBuilder().setBoolValue(value.getBoolValue()).build();
      case INT_VALUE:
        return FlatValue.newBuilder().setIntValue(value.getIntValue()).build();
      default:
        return FlatValue.getDefaultInstance();
    }
  }

  /** Returns the name of the value's oneof case, for messages. */
  static String kindName(FlatValue value) {
    return value.getKindCase() == FlatValue.KindCase.KIND_NOT_SET
        ? "no value"
        : value.getKindCase().name().toLowerCase(Locale.ROOT);
  }

  /** Builds the exception for a value that does not fit a field. */
  static QueryCompileException mismatch(FieldInfo field, FlatValue value) {
    return new QueryCompileException(
        "Field '" + field.path() + "' is mapped as " + field.kind() + " and cannot take "
            + kindName(value) + describe(value),
        field.path());
  }

  private static String describe(FlatValue value) {
    return value.getKindCase() == FlatValue.KindCase.STRING_VALUE
        ? " \"" + value.getStringValue() + "\""
        : "";
  }

  private static boolean isIntegral(double number) {
    return !Double.isInfinite(number)
        && number == Math.rint(number)
        && Math.abs(number) < 9.007199254740992E15;
  }

  private static String numberText(double number) {
    return isIntegral(number) ? Long.toString((long) number) : Double.toString(number);
  }
}
