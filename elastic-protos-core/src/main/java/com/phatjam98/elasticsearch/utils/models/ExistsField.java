package com.phatjam98.elasticsearch.utils.models;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryVariant;

/**
 * A Field which creates a {@link Query} used to build Elasticsearch Exists queries.
 *
 * @see Field
 */
public class ExistsField extends Field {

  /**
   * Uses {@link ExistsField#name} to create the
   * {@link Query}
   * used for Exists queries. Output example:
   * <pre>{@code {
   *    "exists": { "field": "first_name" }
   * }}</pre>
   */
  @Override
  public Query queryBuilder() {
    QueryVariant queryVariant = null;
    var query = QueryBuilders.exists().field(getName()).build();

    if (isNested()) {
      queryVariant = nestedQuery(query);
    } else {
      queryVariant = query;
    }
    return  queryVariant._toQuery();
  }
}
