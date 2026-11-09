package com.phatjam98.elasticprotos.mapping;

import com.phatjam98.elasticprotos.proto.DynamicMode;
import java.util.Objects;

/**
 * Library-wide defaults applied before proto options and programmatic overrides.
 *
 * <p>Start from {@link #standard()} and change single settings with the {@code with*} methods, or
 * set several at once with {@link #builder()} or {@link #toBuilder()}:
 *
 * <pre>{@code
 * MappingDefaults defaults = MappingDefaults.builder()
 *     .stringsAsText(true)
 *     .ignoreAbove(512)
 *     .build();
 * }</pre>
 *
 * <p>Settings:
 *
 * <ul>
 *   <li>{@code stringsAsText}: map plain strings to {@code text} with a keyword sub-field instead
 *       of {@code keyword}; default false
 *   <li>{@code keywordSubfield}: give {@code text} fields a {@code .keyword} sub-field unless the
 *       field says otherwise; default true
 *   <li>{@code ignoreAbove}: {@code ignore_above} for keyword fields and sub-fields; default 256
 *   <li>{@code dateFormat}: {@code format} for date fields that declare none; default {@code
 *       strict_date_optional_time}
 *   <li>{@code maxDepth}: how deep nested messages are expanded before being cut off; default 5
 *   <li>{@code dynamic}: the root dynamic mode when the message declares none; default strict
 *   <li>{@code singularMessage}: how a singular message field is mapped ({@code OBJECT}, the
 *       default, or {@code NESTED})
 *   <li>{@code repeatedMessage}: how a repeated message field is mapped ({@code OBJECT} or {@code
 *       NESTED}, the default)
 *   <li>{@code structKind}: how {@code google.protobuf.Struct} and friends are mapped ({@code
 *       FLATTENED}, the default, or {@code OBJECT})
 *   <li>{@code maxNestedFields}: the most {@code nested} fields a mapping may have, as the engines'
 *       {@code index.mapping.nested_fields.limit} (default 50); 0 disables the check
 *   <li>{@code maxTotalFields}: the most fields a mapping may have, counted like the engines'
 *       {@code index.mapping.total_fields.limit} (default 1000): every mapped field, including
 *       object and nested containers and multi-fields; 0 disables the check
 * </ul>
 *
 * <p>Null settings throw {@link NullPointerException}; negative counts and field kinds other than
 * the ones listed throw {@link IllegalArgumentException}.
 */
public final class MappingDefaults {

  /** The engines' default {@code index.mapping.nested_fields.limit}. */
  public static final int DEFAULT_MAX_NESTED_FIELDS = 50;

  /** The engines' default {@code index.mapping.total_fields.limit}. */
  public static final int DEFAULT_MAX_TOTAL_FIELDS = 1000;

  private final boolean stringsAsText;
  private final boolean keywordSubfield;
  private final int ignoreAbove;
  private final String dateFormat;
  private final int maxDepth;
  private final DynamicMode dynamic;
  private final FieldKind singularMessage;
  private final FieldKind repeatedMessage;
  private final FieldKind structKind;
  private final int maxNestedFields;
  private final int maxTotalFields;

  private MappingDefaults(Builder builder) {
    Objects.requireNonNull(builder.dateFormat, "dateFormat");
    Objects.requireNonNull(builder.dynamic, "dynamic");
    Objects.requireNonNull(builder.singularMessage, "singularMessage");
    Objects.requireNonNull(builder.repeatedMessage, "repeatedMessage");
    Objects.requireNonNull(builder.structKind, "structKind");
    requireMessageKind("singularMessage", builder.singularMessage);
    requireMessageKind("repeatedMessage", builder.repeatedMessage);
    if (builder.structKind != FieldKind.FLATTENED && builder.structKind != FieldKind.OBJECT) {
      throw new IllegalArgumentException(
          "structKind must be FLATTENED or OBJECT: " + builder.structKind);
    }
    requireNotNegative("ignoreAbove", builder.ignoreAbove);
    requireNotNegative("maxDepth", builder.maxDepth);
    requireNotNegative("maxNestedFields", builder.maxNestedFields);
    requireNotNegative("maxTotalFields", builder.maxTotalFields);
    this.stringsAsText = builder.stringsAsText;
    this.keywordSubfield = builder.keywordSubfield;
    this.ignoreAbove = builder.ignoreAbove;
    this.dateFormat = builder.dateFormat;
    this.maxDepth = builder.maxDepth;
    this.dynamic = builder.dynamic;
    this.singularMessage = builder.singularMessage;
    this.repeatedMessage = builder.repeatedMessage;
    this.structKind = builder.structKind;
    this.maxNestedFields = builder.maxNestedFields;
    this.maxTotalFields = builder.maxTotalFields;
  }

  private static void requireMessageKind(String name, FieldKind kind) {
    if (kind != FieldKind.OBJECT && kind != FieldKind.NESTED) {
      throw new IllegalArgumentException(name + " must be OBJECT or NESTED: " + kind);
    }
  }

  private static void requireNotNegative(String name, int value) {
    if (value < 0) {
      throw new IllegalArgumentException(name + " must not be negative: " + value);
    }
  }

  /**
   * The standard defaults: keyword strings, object singulars, nested repeateds, strict dynamic,
   * and the engines' default field limits.
   */
  public static MappingDefaults standard() {
    return builder().build();
  }

  /** Returns a builder holding the {@link #standard()} defaults. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns a builder holding these defaults. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /** Returns whether plain strings map to {@code text} instead of {@code keyword}. */
  public boolean stringsAsText() {
    return stringsAsText;
  }

  /** Returns whether {@code text} fields get a {@code .keyword} sub-field. */
  public boolean keywordSubfield() {
    return keywordSubfield;
  }

  /** Returns {@code ignore_above} for keyword fields and sub-fields. */
  public int ignoreAbove() {
    return ignoreAbove;
  }

  /** Returns the {@code format} of date fields that declare none. */
  public String dateFormat() {
    return dateFormat;
  }

  /** Returns how deep nested messages are expanded. */
  public int maxDepth() {
    return maxDepth;
  }

  /** Returns the root dynamic mode when the message declares none. */
  public DynamicMode dynamic() {
    return dynamic;
  }

  /** Returns how a singular message field is mapped. */
  public FieldKind singularMessage() {
    return singularMessage;
  }

  /** Returns how a repeated message field is mapped. */
  public FieldKind repeatedMessage() {
    return repeatedMessage;
  }

  /** Returns how {@code google.protobuf.Struct} and friends are mapped. */
  public FieldKind structKind() {
    return structKind;
  }

  /** Returns the most {@code nested} fields a mapping may have; 0 means unchecked. */
  public int maxNestedFields() {
    return maxNestedFields;
  }

  /** Returns the most fields a mapping may have; 0 means unchecked. */
  public int maxTotalFields() {
    return maxTotalFields;
  }

  /** Returns a copy with {@code stringsAsText} changed. */
  public MappingDefaults withStringsAsText(boolean value) {
    return toBuilder().stringsAsText(value).build();
  }

  /** Returns a copy with {@code keywordSubfield} changed. */
  public MappingDefaults withKeywordSubfield(boolean value) {
    return toBuilder().keywordSubfield(value).build();
  }

  /** Returns a copy with {@code ignoreAbove} changed. */
  public MappingDefaults withIgnoreAbove(int value) {
    return toBuilder().ignoreAbove(value).build();
  }

  /** Returns a copy with {@code dateFormat} changed. */
  public MappingDefaults withDateFormat(String value) {
    return toBuilder().dateFormat(value).build();
  }

  /** Returns a copy with {@code maxDepth} changed. */
  public MappingDefaults withMaxDepth(int value) {
    return toBuilder().maxDepth(value).build();
  }

  /** Returns a copy with {@code dynamic} changed. */
  public MappingDefaults withDynamic(DynamicMode value) {
    return toBuilder().dynamic(value).build();
  }

  /** Returns a copy with {@code singularMessage} changed. */
  public MappingDefaults withSingularMessage(FieldKind value) {
    return toBuilder().singularMessage(value).build();
  }

  /** Returns a copy with {@code repeatedMessage} changed. */
  public MappingDefaults withRepeatedMessage(FieldKind value) {
    return toBuilder().repeatedMessage(value).build();
  }

  /** Returns a copy with {@code structKind} changed. */
  public MappingDefaults withStructKind(FieldKind value) {
    return toBuilder().structKind(value).build();
  }

  /** Returns a copy with {@code maxNestedFields} changed; 0 disables the check. */
  public MappingDefaults withMaxNestedFields(int value) {
    return toBuilder().maxNestedFields(value).build();
  }

  /** Returns a copy with {@code maxTotalFields} changed; 0 disables the check. */
  public MappingDefaults withMaxTotalFields(int value) {
    return toBuilder().maxTotalFields(value).build();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof MappingDefaults that
        && stringsAsText == that.stringsAsText
        && keywordSubfield == that.keywordSubfield
        && ignoreAbove == that.ignoreAbove
        && dateFormat.equals(that.dateFormat)
        && maxDepth == that.maxDepth
        && dynamic == that.dynamic
        && singularMessage == that.singularMessage
        && repeatedMessage == that.repeatedMessage
        && structKind == that.structKind
        && maxNestedFields == that.maxNestedFields
        && maxTotalFields == that.maxTotalFields;
  }

  @Override
  public int hashCode() {
    return Objects.hash(stringsAsText, keywordSubfield, ignoreAbove, dateFormat, maxDepth, dynamic,
        singularMessage, repeatedMessage, structKind, maxNestedFields, maxTotalFields);
  }

  @Override
  public String toString() {
    return "MappingDefaults[stringsAsText=" + stringsAsText
        + ", keywordSubfield=" + keywordSubfield
        + ", ignoreAbove=" + ignoreAbove
        + ", dateFormat=" + dateFormat
        + ", maxDepth=" + maxDepth
        + ", dynamic=" + dynamic
        + ", singularMessage=" + singularMessage
        + ", repeatedMessage=" + repeatedMessage
        + ", structKind=" + structKind
        + ", maxNestedFields=" + maxNestedFields
        + ", maxTotalFields=" + maxTotalFields + "]";
  }

  /** Builds {@link MappingDefaults}; starts from the {@link #standard()} defaults. */
  public static final class Builder {

    private boolean stringsAsText = false;
    private boolean keywordSubfield = true;
    private int ignoreAbove = 256;
    private String dateFormat = "strict_date_optional_time";
    private int maxDepth = 5;
    private DynamicMode dynamic = DynamicMode.DYNAMIC_STRICT;
    private FieldKind singularMessage = FieldKind.OBJECT;
    private FieldKind repeatedMessage = FieldKind.NESTED;
    private FieldKind structKind = FieldKind.FLATTENED;
    private int maxNestedFields = DEFAULT_MAX_NESTED_FIELDS;
    private int maxTotalFields = DEFAULT_MAX_TOTAL_FIELDS;

    private Builder() {}

    private Builder(MappingDefaults defaults) {
      this.stringsAsText = defaults.stringsAsText;
      this.keywordSubfield = defaults.keywordSubfield;
      this.ignoreAbove = defaults.ignoreAbove;
      this.dateFormat = defaults.dateFormat;
      this.maxDepth = defaults.maxDepth;
      this.dynamic = defaults.dynamic;
      this.singularMessage = defaults.singularMessage;
      this.repeatedMessage = defaults.repeatedMessage;
      this.structKind = defaults.structKind;
      this.maxNestedFields = defaults.maxNestedFields;
      this.maxTotalFields = defaults.maxTotalFields;
    }

    /** Sets whether plain strings map to {@code text}; default false. */
    public Builder stringsAsText(boolean value) {
      this.stringsAsText = value;
      return this;
    }

    /** Sets whether {@code text} fields get a {@code .keyword} sub-field; default true. */
    public Builder keywordSubfield(boolean value) {
      this.keywordSubfield = value;
      return this;
    }

    /** Sets {@code ignore_above} for keyword fields and sub-fields; default 256. */
    public Builder ignoreAbove(int value) {
      this.ignoreAbove = value;
      return this;
    }

    /** Sets the {@code format} of date fields that declare none. */
    public Builder dateFormat(String value) {
      this.dateFormat = value;
      return this;
    }

    /** Sets how deep nested messages are expanded; default 5. */
    public Builder maxDepth(int value) {
      this.maxDepth = value;
      return this;
    }

    /** Sets the root dynamic mode when the message declares none; default strict. */
    public Builder dynamic(DynamicMode value) {
      this.dynamic = value;
      return this;
    }

    /** Sets how a singular message field is mapped; default {@code OBJECT}. */
    public Builder singularMessage(FieldKind value) {
      this.singularMessage = value;
      return this;
    }

    /** Sets how a repeated message field is mapped; default {@code NESTED}. */
    public Builder repeatedMessage(FieldKind value) {
      this.repeatedMessage = value;
      return this;
    }

    /** Sets how {@code google.protobuf.Struct} and friends are mapped; default FLATTENED. */
    public Builder structKind(FieldKind value) {
      this.structKind = value;
      return this;
    }

    /** Sets the most {@code nested} fields a mapping may have; 0 disables the check. */
    public Builder maxNestedFields(int value) {
      this.maxNestedFields = value;
      return this;
    }

    /** Sets the most fields a mapping may have; 0 disables the check. */
    public Builder maxTotalFields(int value) {
      this.maxTotalFields = value;
      return this;
    }

    /**
     * Builds the defaults.
     *
     * @return the defaults
     * @throws NullPointerException when the date format, dynamic mode or a field kind is null
     * @throws IllegalArgumentException when a field kind is not allowed for its setting, or a
     *     count is negative
     */
    public MappingDefaults build() {
      return new MappingDefaults(this);
    }
  }
}
