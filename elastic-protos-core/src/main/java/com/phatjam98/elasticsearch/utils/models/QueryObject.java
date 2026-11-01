package com.phatjam98.elasticsearch.utils.models;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Holds a collection of {@link Field} and used to construct Elasticsearch Bool Queries.
 */
public class QueryObject {

  private List<? extends Field> searchFields;

  public List<? extends Field> getSearchFields() {
    return searchFields;
  }

  public QueryObject setSearchFields(List<? extends Field> searchFields) {
    this.searchFields = searchFields;
    return this;
  }

  /**
   * Creates a {@link Query} used to build Elasticsearch Bool Queries.
   *
   * @return Query
   */
  public Query esBoolQuery() {
    var esBoolQuery = QueryBuilders.bool();
    getSearchFields().stream().map(Field::queryBuilder)
        .filter(Objects::nonNull)
        .forEach(searchField -> {
          if (Arrays.asList(Query.Kind.GeoShape, Query.Kind.GeoDistance, Query.Kind.GeoPolygon,
              Query.Kind.GeoBoundingBox).contains(searchField._kind())) {
            esBoolQuery.filter(searchField);
          } else {
            esBoolQuery.must(searchField);
          }
        });

    return esBoolQuery.build()._toQuery();
  }

  public List<Query> esQueries() {
    return getSearchFields().stream().map(Field::queryBuilder)
        .filter(Objects::nonNull).collect(Collectors.toList());
  }
}