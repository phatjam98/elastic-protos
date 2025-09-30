package com.phatjam98.elasticsearch.utils.models;

import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryVariant;

/**
 * A Field which creates a {@link Query} used to build
 * Elasticsearch match queries.
 *
 * @see Field
 */
public class MatchesField extends Field {

  /**
   * Uses {@link ContainsField#value} and {@link ContainsField#name} to create the
   * {@link Query} used for match queries. Output example:
   * <pre>{@code {
   *   "query_string" : {
   *     "query" : "Joh*",
   *     "fields" : [
   *       "first_name^1.0"
   *     ],
   *     "type" : "best_fields",
   *     "default_operator" : "and",
   *     "max_determinized_states" : 10000,
   *     "enable_position_increments" : true,
   *     "fuzziness" : "AUTO",
   *     "fuzzy_prefix_length" : 0,
   *     "fuzzy_max_expansions" : 50,
   *     "phrase_slop" : 0,
   *     "escape" : false,
   *     "auto_generate_synonyms_phrase_query" : true,
   *     "fuzzy_transpositions" : true,
   *     "boost" : 1.0
   *   }
   * }}</pre>
   *
   * @return QueryStringQueryBuilder
   */
  @Override
  public Query queryBuilder() {
    QueryVariant queryVariant = null;
    String value = esEscapeString(valueString(getValue()));

    if (!value.matches(".*[\\*~].*")) {
      value = value + "*";
    }

    var query =
        QueryBuilders.queryString().query(value).defaultOperator(Operator.And).fields(getName())
            .build();

    if (isNested()) {
      queryVariant = nestedQuery(query);
    } else {
      queryVariant = query;
    }

    return queryVariant._toQuery();
  }
}
