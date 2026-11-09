package com.phatjam98.elasticprotos.mapping;

import com.phatjam98.elasticprotos.proto.DynamicMode;

/**
 * Library-wide defaults applied before proto options and programmatic overrides.
 *
 * @param stringsAsText map plain strings to {@code text} with a keyword sub-field instead of
 *     {@code keyword}
 * @param keywordSubfield give {@code text} fields a {@code .keyword} sub-field unless the field
 *     says otherwise
 * @param ignoreAbove {@code ignore_above} for keyword fields and sub-fields
 * @param dateFormat {@code format} for date fields that declare none
 * @param maxDepth how deep nested messages are expanded before being cut off
 * @param dynamic the root dynamic mode when the message declares none
 * @param singularMessage how a singular message field is mapped ({@code OBJECT} or {@code NESTED})
 * @param repeatedMessage how a repeated message field is mapped ({@code OBJECT} or {@code NESTED})
 * @param structKind how {@code google.protobuf.Struct} and friends are mapped ({@code FLATTENED} or
 *     {@code OBJECT})
 * @param maxNestedFields the most {@code nested} fields a mapping may have, as the engines'
 *     {@code index.mapping.nested_fields.limit} (default 50); 0 disables the check
 * @param maxTotalFields the most fields a mapping may have, counted like the engines' {@code
 *     index.mapping.total_fields.limit} (default 1000): every mapped field, including object and
 *     nested containers and multi-fields; 0 disables the check
 */
public record MappingDefaults(
    boolean stringsAsText,
    boolean keywordSubfield,
    int ignoreAbove,
    String dateFormat,
    int maxDepth,
    DynamicMode dynamic,
    FieldKind singularMessage,
    FieldKind repeatedMessage,
    FieldKind structKind,
    int maxNestedFields,
    int maxTotalFields) {

  /** The engines' default {@code index.mapping.nested_fields.limit}. */
  public static final int DEFAULT_MAX_NESTED_FIELDS = 50;

  /** The engines' default {@code index.mapping.total_fields.limit}. */
  public static final int DEFAULT_MAX_TOTAL_FIELDS = 1000;

  /**
   * The standard defaults: keyword strings, object singulars, nested repeateds, strict dynamic,
   * and the engines' default field limits.
   */
  public static MappingDefaults standard() {
    return new MappingDefaults(
        false,
        true,
        256,
        "strict_date_optional_time",
        5,
        DynamicMode.DYNAMIC_STRICT,
        FieldKind.OBJECT,
        FieldKind.NESTED,
        FieldKind.FLATTENED,
        DEFAULT_MAX_NESTED_FIELDS,
        DEFAULT_MAX_TOTAL_FIELDS);
  }

  /** Returns a copy with {@code stringsAsText} changed. */
  public MappingDefaults withStringsAsText(boolean value) {
    return new MappingDefaults(value, keywordSubfield, ignoreAbove, dateFormat, maxDepth,
        dynamic, singularMessage, repeatedMessage, structKind, maxNestedFields, maxTotalFields);
  }

  /** Returns a copy with {@code keywordSubfield} changed. */
  public MappingDefaults withKeywordSubfield(boolean value) {
    return new MappingDefaults(stringsAsText, value, ignoreAbove, dateFormat, maxDepth,
        dynamic, singularMessage, repeatedMessage, structKind, maxNestedFields, maxTotalFields);
  }

  /** Returns a copy with {@code ignoreAbove} changed. */
  public MappingDefaults withIgnoreAbove(int value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, value, dateFormat, maxDepth,
        dynamic, singularMessage, repeatedMessage, structKind, maxNestedFields, maxTotalFields);
  }

  /** Returns a copy with {@code dateFormat} changed. */
  public MappingDefaults withDateFormat(String value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, value, maxDepth,
        dynamic, singularMessage, repeatedMessage, structKind, maxNestedFields, maxTotalFields);
  }

  /** Returns a copy with {@code maxDepth} changed. */
  public MappingDefaults withMaxDepth(int value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, dateFormat, value,
        dynamic, singularMessage, repeatedMessage, structKind, maxNestedFields, maxTotalFields);
  }

  /** Returns a copy with {@code dynamic} changed. */
  public MappingDefaults withDynamic(DynamicMode value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, dateFormat, maxDepth,
        value, singularMessage, repeatedMessage, structKind, maxNestedFields, maxTotalFields);
  }

  /** Returns a copy with {@code singularMessage} changed. */
  public MappingDefaults withSingularMessage(FieldKind value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, dateFormat, maxDepth,
        dynamic, value, repeatedMessage, structKind, maxNestedFields, maxTotalFields);
  }

  /** Returns a copy with {@code repeatedMessage} changed. */
  public MappingDefaults withRepeatedMessage(FieldKind value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, dateFormat, maxDepth,
        dynamic, singularMessage, value, structKind, maxNestedFields, maxTotalFields);
  }

  /** Returns a copy with {@code structKind} changed. */
  public MappingDefaults withStructKind(FieldKind value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, dateFormat, maxDepth,
        dynamic, singularMessage, repeatedMessage, value, maxNestedFields, maxTotalFields);
  }

  /** Returns a copy with {@code maxNestedFields} changed; 0 disables the check. */
  public MappingDefaults withMaxNestedFields(int value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, dateFormat, maxDepth,
        dynamic, singularMessage, repeatedMessage, structKind, value, maxTotalFields);
  }

  /** Returns a copy with {@code maxTotalFields} changed; 0 disables the check. */
  public MappingDefaults withMaxTotalFields(int value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, dateFormat, maxDepth,
        dynamic, singularMessage, repeatedMessage, structKind, maxNestedFields, value);
  }
}
