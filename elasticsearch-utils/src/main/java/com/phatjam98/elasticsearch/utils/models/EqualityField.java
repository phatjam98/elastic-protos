package com.phatjam98.elasticsearch.utils.models;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryVariant;

/**
 * A Field which creates a {@link Query} used to build Elasticsearch Equality queries.
 *
 * @see Field
 */
public class EqualityField extends Field {

  /**
   * Uses {@link EqualityField#value} and {@link EqualityField#name} to create the
   * {@link Query}
   * used for Equality queries. Output example:
   * <pre>{@code {
   *   "constant_score" : {
   *     "filter" : {
   *       "term" : {
   *         "first_name" : {
   *           "value" : "John",
   *           "boost" : 1.0
   *         }
   *       }
   *     },
   *     "boost" : 1.0
   *   }
   * }}</pre>
   */
  @Override
  public Query queryBuilder() {
    QueryVariant queryVariant = null;
    var query = QueryBuilders.term().field(getName()).value(valueString(getValue())).build();

    if (isNested()) {
      queryVariant = nestedQuery(query);
    } else {
      queryVariant = query;
    }
    return  queryVariant._toQuery();
  }
}
