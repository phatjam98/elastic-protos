package com.phatjam98.elasticsearch.utils.models;

import com.phatjam98.core.common.proto.FlatStructProtos.FlatValue.KindCase;
import java.util.Objects;

/**
 * An Object used to define a field value and type.
 */
public class FieldValue {
  private KindCase kind;
  private Object value;

  public KindCase getKind() {
    return kind;
  }

  public FieldValue setKind(KindCase kind) {
    this.kind = kind;
    return this;
  }

  public Object getValue() {
    return value;
  }

  public FieldValue setValue(Object value) {
    this.value = value;
    return this;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldValue that = (FieldValue) o;
    return kind == that.kind && value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kind, value);
  }
}