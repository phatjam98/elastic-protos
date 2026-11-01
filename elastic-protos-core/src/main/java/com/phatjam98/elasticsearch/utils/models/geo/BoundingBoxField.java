package com.phatjam98.elasticsearch.utils.models.geo;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import com.phatjam98.core.common.proto.FlatStructProtos;
import com.phatjam98.elasticsearch.utils.models.Field;

/**
 * A Field which creates a {@link Query} used to build Elasticsearch GEO_BOUNDING_BOX
 * queries from {@link Field}.
 */
public class BoundingBoxField extends Field {

  /**
   * Uses {@link FlatStructProtos.GeoBoundingBoxValue} to
   * create the {@link Query} used for GEO_BOUNDING_BOX queries. Output example:
   * <pre>{@code {
   *   "geo_bounding_box" : {
   *     "centroids" : {
   *       "top_left" : [
   *         -83.58,
   *         29.11
   *       ],
   *       "bottom_right" : [
   *         -79.57,
   *         27.49
   *       ]
   *     },
   *     "validation_method" : "STRICT",
   *     "type" : "MEMORY",
   *     "ignore_unmapped" : false,
   *     "boost" : 1.0
   *   }
   * }}</pre>
   *
   * @return QueryBuilder
   */
  @Override
  public Query queryBuilder() {
    FlatStructProtos.GeoBoundingBoxValue geoValue =
        (FlatStructProtos.GeoBoundingBoxValue) getValue().getValue();

    Query query = null;

    var queryBuilder = QueryBuilders.geoBoundingBox();
    queryBuilder.field(getName());
    queryBuilder.boundingBox(bb -> {
      return bb.coords(c -> c.bottom(geoValue.getBottomRight().getLat())
          .right(geoValue.getBottomRight().getLon())
          .top(geoValue.getTopLeft().getLat())
          .left(geoValue.getTopLeft().getLon()));
    });

    if (isNested()) {
      query = nestedQuery(queryBuilder.build())._toQuery();
    } else {
      query = queryBuilder.build()._toQuery();
    }

    return query;
  }
}
