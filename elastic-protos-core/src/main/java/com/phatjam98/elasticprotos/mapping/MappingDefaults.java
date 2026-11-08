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
    FieldKind structKind) {

  /** The standard defaults: keyword strings, object singulars, nested repeateds, strict dynamic. */
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
        FieldKind.FLATTENED);
  }

  /** Returns a copy with {@code stringsAsText} changed. */
  public MappingDefaults withStringsAsText(boolean value) {
    return new MappingDefaults(value, keywordSubfield, ignoreAbove, dateFormat, maxDepth,
        dynamic, singularMessage, repeatedMessage, structKind);
  }

  /** Returns a copy with {@code keywordSubfield} changed. */
  public MappingDefaults withKeywordSubfield(boolean value) {
    return new MappingDefaults(stringsAsText, value, ignoreAbove, dateFormat, maxDepth,
        dynamic, singularMessage, repeatedMessage, structKind);
  }

  /** Returns a copy with {@code ignoreAbove} changed. */
  public MappingDefaults withIgnoreAbove(int value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, value, dateFormat, maxDepth,
        dynamic, singularMessage, repeatedMessage, structKind);
  }

  /** Returns a copy with {@code dateFormat} changed. */
  public MappingDefaults withDateFormat(String value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, value, maxDepth,
        dynamic, singularMessage, repeatedMessage, structKind);
  }

  /** Returns a copy with {@code maxDepth} changed. */
  public MappingDefaults withMaxDepth(int value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, dateFormat, value,
        dynamic, singularMessage, repeatedMessage, structKind);
  }

  /** Returns a copy with {@code dynamic} changed. */
  public MappingDefaults withDynamic(DynamicMode value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, dateFormat, maxDepth,
        value, singularMessage, repeatedMessage, structKind);
  }

  /** Returns a copy with {@code singularMessage} changed. */
  public MappingDefaults withSingularMessage(FieldKind value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, dateFormat, maxDepth,
        dynamic, value, repeatedMessage, structKind);
  }

  /** Returns a copy with {@code repeatedMessage} changed. */
  public MappingDefaults withRepeatedMessage(FieldKind value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, dateFormat, maxDepth,
        dynamic, singularMessage, value, structKind);
  }

  /** Returns a copy with {@code structKind} changed. */
  public MappingDefaults withStructKind(FieldKind value) {
    return new MappingDefaults(stringsAsText, keywordSubfield, ignoreAbove, dateFormat, maxDepth,
        dynamic, singularMessage, repeatedMessage, value);
  }
}
