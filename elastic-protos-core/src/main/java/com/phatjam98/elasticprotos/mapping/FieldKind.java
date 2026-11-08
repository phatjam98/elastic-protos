package com.phatjam98.elasticprotos.mapping;

import com.phatjam98.elasticprotos.proto.EsType;

/** The engine field types the mapper can emit, with their mapping type names. */
public enum FieldKind {
  KEYWORD("keyword"),
  TEXT("text"),
  SEARCH_AS_YOU_TYPE("search_as_you_type"),
  INTEGER("integer"),
  LONG("long"),
  DOUBLE("double"),
  FLOAT("float"),
  BOOLEAN("boolean"),
  DATE("date"),
  DATE_NANOS("date_nanos"),
  GEO_POINT("geo_point"),
  GEO_SHAPE("geo_shape"),
  OBJECT("object"),
  NESTED("nested"),
  FLATTENED("flattened"),
  BINARY("binary"),
  IP("ip");

  private final String esType;

  FieldKind(String esType) {
    this.esType = esType;
  }

  /** Returns the value used for {@code "type"} in the mapping. */
  public String esType() {
    return esType;
  }

  /** Returns whether this kind is analyzed text. */
  public boolean isText() {
    return this == TEXT || this == SEARCH_AS_YOU_TYPE;
  }

  /** Returns whether this kind holds a number. */
  public boolean isNumeric() {
    return this == INTEGER || this == LONG || this == DOUBLE || this == FLOAT;
  }

  /** Returns whether this kind holds a date. */
  public boolean isDate() {
    return this == DATE || this == DATE_NANOS;
  }

  /** Returns whether this kind contains sub-properties. */
  public boolean isContainer() {
    return this == OBJECT || this == NESTED;
  }

  /**
   * Maps an explicitly requested option type to a kind.
   *
   * @param type a value other than {@code AUTO} or {@code IGNORE}
   * @return the kind
   * @throws IllegalArgumentException for {@code AUTO}, {@code IGNORE} or unknown values
   */
  public static FieldKind fromEsType(EsType type) {
    switch (type) {
      case KEYWORD:
        return KEYWORD;
      case TEXT:
        return TEXT;
      case SEARCH_AS_YOU_TYPE:
        return SEARCH_AS_YOU_TYPE;
      case INTEGER:
        return INTEGER;
      case LONG:
        return LONG;
      case DOUBLE:
        return DOUBLE;
      case FLOAT:
        return FLOAT;
      case BOOLEAN:
        return BOOLEAN;
      case DATE:
        return DATE;
      case DATE_NANOS:
        return DATE_NANOS;
      case GEO_POINT:
        return GEO_POINT;
      case GEO_SHAPE:
        return GEO_SHAPE;
      case OBJECT:
        return OBJECT;
      case NESTED:
        return NESTED;
      case FLATTENED:
        return FLATTENED;
      case BINARY:
        return BINARY;
      case IP:
        return IP;
      default:
        throw new IllegalArgumentException("No field kind for option type " + type);
    }
  }
}
