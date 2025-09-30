package com.phatjam98.elasticsearch.utils.models.geo;

import co.elastic.clients.elasticsearch._types.GeoDistanceType;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import com.phatjam98.core.common.proto.FlatStructProtos;
import com.phatjam98.elasticsearch.utils.models.Field;

/**
 * A Field which creates a {@link Query} used to build Elasticsearch GEO_DISTANCE queries
 * from {@link Field}.
 */
public class DistanceField extends Field {

  /**
   * Uses {@link FlatStructProtos.GeoDistanceValue} to
   * create the {@link Query} used for GEO_DISTANCE queries. Output example:
   * <pre>{@code {
   *   "geo_distance" : {
   *     "centroids" : [
   *       -81.3867,
   *       28.5423
   *     ],
   *     "distance" : 10000.0,
   *     "distance_type" : "arc",
   *     "validation_method" : "STRICT",
   *     "ignore_unmapped" : false,
   *     "boost" : 1.0
   *   }
   * }}</pre>
   *
   * @return QueryBuilder
   */
  @Override
  public Query queryBuilder() {
    FlatStructProtos.GeoDistanceValue geoValue =
        (FlatStructProtos.GeoDistanceValue) getValue().getValue();

    Query query = null;

    if (geoValue != null) {
      var queryBuilder = QueryBuilders.geoDistance();
      queryBuilder.field(getName());
      queryBuilder.distance(geoValue.getDistance());
      if (geoValue.getDistanceType() == FlatStructProtos.DistanceType.ARC) {
        queryBuilder.distanceType(GeoDistanceType.Arc);
      } else if (geoValue.getDistanceType() == FlatStructProtos.DistanceType.PLANE) {
        queryBuilder.distanceType(GeoDistanceType.Plane);
      }

      queryBuilder.location(l -> l.latlon(
          ll -> ll.lat(geoValue.getPoint().getLat()).lon(geoValue.getPoint().getLon())));

      if (isNested()) {
        query = nestedQuery(queryBuilder.build())._toQuery();
      } else {
        query = queryBuilder.build()._toQuery();
      }
    }

    return query;
  }
}
