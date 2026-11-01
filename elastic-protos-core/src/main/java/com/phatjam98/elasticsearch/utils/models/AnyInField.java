package com.phatjam98.elasticsearch.utils.models;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryVariant;
import com.phatjam98.core.common.proto.FlatStructProtos;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A Field which creates a {@link Query} used to build Elasticsearch AnyIn
 * queries from {@link Field}.
 */
public class AnyInField extends Field {

  /**
   * Uses {@link FlatStructProtos.PrimitiveList} to create
   * the {@link Query} used for ANY_IN queries. Output example:
   * <pre>{@code {
   *   "constant_score" : {
   *     "filter" : {
   *       "terms" : {
   *         "first_name" : [
   *           "steve",
   *           "bob",
   *           "dave"
   *         ],
   *         "boost" : 1.0
   *       }
   *     },
   *     "boost" : 1.0
   *   }
   * }}</pre>
   *
   * @return QueryBuilder
   */
  @Override
  public Query queryBuilder() {
    QueryVariant queryVariant = null;
    Object value = getValue().getValue();
    FlatStructProtos.PrimitiveList primitiveList;

    if (value instanceof FlatStructProtos.PrimitiveList) {
      primitiveList = (FlatStructProtos.PrimitiveList) value;
    } else {
      throw new IllegalArgumentException("Invalid value for ANY_IN: " + value.getClass());
    }

    var query = QueryBuilders.terms().queryName(getName()).field(getName())
        .terms(t -> t.value(extractFieldValues(primitiveList))).build();

    if (isNested()) {
      queryVariant = nestedQuery(query);
    } else {
      queryVariant = query;
    }

    return queryVariant._toQuery();
  }

  private List<co.elastic.clients.elasticsearch._types.FieldValue> extractFieldValues(
      FlatStructProtos.PrimitiveList primitiveList) {
    return primitiveList.getValuesList().stream().map(value -> {
      switch (value.getKindCase()) {
        case BOOL_VALUE:
          return new co.elastic.clients.elasticsearch._types.FieldValue.Builder()
              .booleanValue(value.getBoolValue()).build();
        case NUMBER_VALUE:
          return new co.elastic.clients.elasticsearch._types.FieldValue.Builder()
              .doubleValue(value.getNumberValue()).build();
        case STRING_VALUE:
          return new co.elastic.clients.elasticsearch._types.FieldValue.Builder()
              .stringValue(value.getStringValue()).build();
        default:
          throw new IllegalArgumentException(
              "Invalid value type for ANY_IN: " + value.getKindCase());
      }
    }).collect(Collectors.toList());
  }
}
