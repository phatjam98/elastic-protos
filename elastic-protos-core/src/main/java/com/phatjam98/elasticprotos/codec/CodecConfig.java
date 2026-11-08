package com.phatjam98.elasticprotos.codec;

import com.google.protobuf.TypeRegistry;

/**
 * Codec settings.
 *
 * @param includeDefaults also write proto3 fields that hold their default value; by default they
 *     are omitted so that an {@code exists} query means "was set"
 * @param int64AsNumber write 64-bit integers as JSON numbers rather than the strings protobuf's
 *     JSON mapping uses; values beyond 2^53 lose precision in JavaScript consumers
 * @param unknownFields what to do with unknown document keys when reading back
 * @param anyTypes the type registry used to serialize {@code google.protobuf.Any} fields that are
 *     mapped explicitly rather than ignored
 */
public record CodecConfig(
    boolean includeDefaults,
    boolean int64AsNumber,
    UnknownFieldPolicy unknownFields,
    TypeRegistry anyTypes) {

  /** Omit defaults, 64-bit integers as numbers, ignore unknown keys, no Any types. */
  public static CodecConfig standard() {
    return new CodecConfig(false, true, UnknownFieldPolicy.IGNORE,
        TypeRegistry.getEmptyTypeRegistry());
  }

  /** Returns a copy with {@code includeDefaults} changed. */
  public CodecConfig withIncludeDefaults(boolean value) {
    return new CodecConfig(value, int64AsNumber, unknownFields, anyTypes);
  }

  /** Returns a copy with {@code int64AsNumber} changed. */
  public CodecConfig withInt64AsNumber(boolean value) {
    return new CodecConfig(includeDefaults, value, unknownFields, anyTypes);
  }

  /** Returns a copy with {@code unknownFields} changed. */
  public CodecConfig withUnknownFields(UnknownFieldPolicy value) {
    return new CodecConfig(includeDefaults, int64AsNumber, value, anyTypes);
  }

  /** Returns a copy with {@code anyTypes} changed. */
  public CodecConfig withAnyTypes(TypeRegistry value) {
    return new CodecConfig(includeDefaults, int64AsNumber, unknownFields, value);
  }
}
