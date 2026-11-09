package com.phatjam98.elasticprotos.codec;

import com.google.protobuf.TypeRegistry;
import java.util.Objects;

/**
 * Codec settings.
 *
 * <p>Start from {@link #standard()} and change single settings with the {@code with*} methods, or
 * set several at once with {@link #builder()} or {@link #toBuilder()}.
 *
 * <p>Settings:
 *
 * <ul>
 *   <li>{@code includeDefaults}: write proto3 fields that hold their default value ({@code false},
 *       {@code 0}, {@code ""}, the first enum value, empty repeated and map fields), so that {@code
 *       EQUALS}, ranges and sorting see default values like any other; true by default. Unset
 *       message fields (sub-messages, timestamps, wrappers) and unset oneof members are never
 *       written. With false, defaults are omitted and {@code EXISTS} means "was set"
 *   <li>{@code int64AsNumber}: write 64-bit integers as JSON numbers rather than the strings
 *       protobuf's JSON mapping uses; values beyond 2^53 lose precision in JavaScript consumers;
 *       true by default
 *   <li>{@code unknownFields}: what to do with unknown document keys when reading back; default
 *       {@link UnknownFieldPolicy#IGNORE}
 *   <li>{@code anyTypes}: the type registry used to serialize {@code google.protobuf.Any} fields
 *       that are mapped explicitly rather than ignored; empty by default
 * </ul>
 */
public final class CodecConfig {

  private final boolean includeDefaults;
  private final boolean int64AsNumber;
  private final UnknownFieldPolicy unknownFields;
  private final TypeRegistry anyTypes;

  private CodecConfig(Builder builder) {
    this.includeDefaults = builder.includeDefaults;
    this.int64AsNumber = builder.int64AsNumber;
    this.unknownFields = builder.unknownFields;
    this.anyTypes = builder.anyTypes;
  }

  /** Include defaults, 64-bit integers as numbers, ignore unknown keys, no Any types. */
  public static CodecConfig standard() {
    return builder().build();
  }

  /** Returns a builder holding the {@link #standard()} settings. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns a builder holding these settings. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /** Returns whether proto3 default values are written. */
  public boolean includeDefaults() {
    return includeDefaults;
  }

  /** Returns whether 64-bit integers are written as JSON numbers. */
  public boolean int64AsNumber() {
    return int64AsNumber;
  }

  /** Returns what happens to unknown document keys when reading back. */
  public UnknownFieldPolicy unknownFields() {
    return unknownFields;
  }

  /** Returns the type registry for {@code google.protobuf.Any} fields. */
  public TypeRegistry anyTypes() {
    return anyTypes;
  }

  /** Returns a copy with {@code includeDefaults} changed. */
  public CodecConfig withIncludeDefaults(boolean value) {
    return toBuilder().includeDefaults(value).build();
  }

  /** Returns a copy with {@code int64AsNumber} changed. */
  public CodecConfig withInt64AsNumber(boolean value) {
    return toBuilder().int64AsNumber(value).build();
  }

  /** Returns a copy with {@code unknownFields} changed. */
  public CodecConfig withUnknownFields(UnknownFieldPolicy value) {
    return toBuilder().unknownFields(value).build();
  }

  /** Returns a copy with {@code anyTypes} changed. */
  public CodecConfig withAnyTypes(TypeRegistry value) {
    return toBuilder().anyTypes(value).build();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof CodecConfig that
        && includeDefaults == that.includeDefaults
        && int64AsNumber == that.int64AsNumber
        && unknownFields == that.unknownFields
        && Objects.equals(anyTypes, that.anyTypes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(includeDefaults, int64AsNumber, unknownFields, anyTypes);
  }

  @Override
  public String toString() {
    return "CodecConfig[includeDefaults=" + includeDefaults
        + ", int64AsNumber=" + int64AsNumber
        + ", unknownFields=" + unknownFields
        + ", anyTypes=" + anyTypes + "]";
  }

  /** Builds a {@link CodecConfig}; starts from the {@link #standard()} settings. */
  public static final class Builder {

    private boolean includeDefaults = true;
    private boolean int64AsNumber = true;
    private UnknownFieldPolicy unknownFields = UnknownFieldPolicy.IGNORE;
    private TypeRegistry anyTypes = TypeRegistry.getEmptyTypeRegistry();

    private Builder() {}

    private Builder(CodecConfig config) {
      this.includeDefaults = config.includeDefaults;
      this.int64AsNumber = config.int64AsNumber;
      this.unknownFields = config.unknownFields;
      this.anyTypes = config.anyTypes;
    }

    /** Sets whether proto3 default values are written; default true. */
    public Builder includeDefaults(boolean value) {
      this.includeDefaults = value;
      return this;
    }

    /** Sets whether 64-bit integers are written as JSON numbers; default true. */
    public Builder int64AsNumber(boolean value) {
      this.int64AsNumber = value;
      return this;
    }

    /** Sets what happens to unknown document keys when reading back; default IGNORE. */
    public Builder unknownFields(UnknownFieldPolicy value) {
      this.unknownFields = value;
      return this;
    }

    /** Sets the type registry for {@code google.protobuf.Any} fields; default empty. */
    public Builder anyTypes(TypeRegistry value) {
      this.anyTypes = value;
      return this;
    }

    /** Builds the settings. */
    public CodecConfig build() {
      return new CodecConfig(this);
    }
  }
}
