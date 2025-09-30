package com.phatjam98.elasticsearch.utils.models;

import co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode;
import co.elastic.clients.elasticsearch._types.query_dsl.NestedQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryStringQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryVariant;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;



/**
 * Abstract class used as the base Field.
 * Extending classes must implement {@link Field#queryBuilder()}
 */
public abstract class Field implements Serializable {
  static List<String>
      esReserved =
      Arrays.asList("\\", "*", "-", "=", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", "\"",
          "?", ":", "/");

  @JsonProperty("json_path")
  private String jsonPath;
  private String name;
  private transient FieldValue value;

  public String getJsonPath() {
    return jsonPath;
  }

  public Field setJsonPath(String jsonPath) {
    this.jsonPath = jsonPath;
    return this;
  }

  public String getName() {
    return name;
  }

  public Field setName(String name) {
    this.name = name;
    return this;
  }

  public FieldValue getValue() {
    return value;
  }

  public Field setValue(FieldValue value) {
    this.value = value;
    return this;
  }

  /**
   * Implement the {@link Query.Builder} for the extending Field type.
   *
   * @return {@link Query}
   */
  public abstract Query queryBuilder();

  /**
   * Used to remove elasticsearch reserved characters from the value and replace with a space.
   *
   * @param value String value for this field.
   * @return String escaped for use with elasticsearch
   */
  public String esEscapeString(String value) {
    String result = value;
    if (result != null) {
      for (String esc : esReserved) {
        result = result.replace(esc, " ");
      }
    }

    return result;
  }

  /**
   * This supports a single depth nested query. For example {@code location.region} would require
   * a NestedQuery.  This requires a path field which is mapped to the top position, in this case
   * location.
   *
   * @see NestedQuery
   * @see NestedQuery.Builder
   *
   * @param query The query to be used in the NestedQuery.
   * @return NestedQuery
   */
  public NestedQuery nestedQuery(QueryVariant query) {
    return QueryBuilders.nested().path(extractPath())
        .query(query._toQuery()).scoreMode(ChildScoreMode.None)
        .build();
  }

  /**
   * This supports a single depth nested query. For example {@code location.region} would require
   * a NestedQuery.  This requires a path field which is mapped to the top position, in this case
   * location.
   *
   * @param query The query to be used in the NestedQuery.
   * @return NestedQuery
   */
  public NestedQuery nestedQuery(QueryStringQuery query) {
    return QueryBuilders.nested().path(extractPath())
        .query(query._toQuery()).scoreMode(ChildScoreMode.None)
        .build();
  }

  public boolean isNested() {
    return getName().contains(".");
  }

  private String extractPath() {
    if (isNested()) {
      return getName().substring(0, getName().lastIndexOf("."));
    } else {
      return getName();
    }
  }

  /**
   * Used to get the String representation of the {@link FieldValue#value}.
   *
   * @param fieldValue FieldValue
   * @return String representation of the value
   */
  public String valueString(FieldValue fieldValue) {
    String result;

    switch (fieldValue.getKind()) {
      case BOOL_VALUE:
        result = "false";

        if (Boolean.TRUE.equals(fieldValue.getValue())) {
          result = "true";
        }
        break;
      case NUMBER_VALUE:
        result = String.valueOf(fieldValue.getValue());
        break;
      case STRING_VALUE:
        result = (String) fieldValue.getValue();
        break;
      default:
        result = null;
    }

    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Field that = (Field) o;
    return jsonPath.equals(that.jsonPath) && name.equals(that.name) && value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jsonPath, name, value);
  }
}