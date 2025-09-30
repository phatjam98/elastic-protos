package com.phatjam98.elasticsearch.utils.models;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryVariant;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.phatjam98.core.common.proto.FlatStructProtos;
import java.text.DecimalFormat;
import java.util.Objects;

/**
 * A Field which creates a {@link Query} used to build Elasticsearch range queries.
 *
 * @see Field
 */
public class RangeField extends Field {

  @JsonProperty("min_inclusive")
  private boolean minInclusive = false;
  @JsonProperty("max_inclusive")
  private boolean maxInclusive = false;

  public boolean isMinInclusive() {
    return minInclusive;
  }

  public RangeField setMinInclusive(boolean minInclusive) {
    this.minInclusive = minInclusive;
    return this;
  }

  public boolean isMaxInclusive() {
    return maxInclusive;
  }

  public RangeField setMaxInclusive(boolean maxInclusive) {
    this.maxInclusive = maxInclusive;
    return this;
  }

  /**
   * Uses {@link RangeField#name} and {@link RangeField#value} to create the
   * {@link Query} use for range queries. Output example:
   * <pre>{@code {
   *   "range" : {
   *     "rating" : {
   *       "from" : "35",
   *       "to" : "95",
   *       "include_lower" : true,
   *       "include_upper" : false,
   *       "boost" : 1.0
   *     }
   *   }
   * }}</pre>
   *
   * @see FlatStructProtos.RangeValue
   *
   * @return RangeQueryBuilder
   */
  @Override
  public Query queryBuilder() {
    QueryVariant queryVariant = null;
    DecimalFormat df = new DecimalFormat("0");
    df.setMaximumFractionDigits(340);
    FlatStructProtos.RangeValue rangeValue = (FlatStructProtos.RangeValue) getValue().getValue();
    var queryBuilder = QueryBuilders.range().field(getName());
    JsonData min = JsonData.of(df.format(rangeValue.getMin()));
    JsonData max = JsonData.of(df.format(rangeValue.getMax()));

    if (rangeValue.getMinInclusive()) {
      queryBuilder.gte(min);
    } else {
      queryBuilder.gt(min);
    }

    if (rangeValue.getMaxInclusive()) {
      queryBuilder.lte(max);
    } else {
      queryBuilder.lt(max);
    }

    if (isNested()) {
      queryVariant = nestedQuery(queryBuilder.build());
    } else {
      queryVariant = queryBuilder.build();
    }

    return queryVariant._toQuery();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RangeField that = (RangeField) o;
    return minInclusive == that.minInclusive && maxInclusive == that.maxInclusive;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), minInclusive, maxInclusive);
  }
}
