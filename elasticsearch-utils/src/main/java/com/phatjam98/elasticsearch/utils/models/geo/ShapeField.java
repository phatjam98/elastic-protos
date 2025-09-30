package com.phatjam98.elasticsearch.utils.models.geo;

import co.elastic.clients.elasticsearch._types.GeoShapeRelation;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoShapeFieldQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.json.JsonData;
import com.phatjam98.core.common.proto.GeoBufProtos;
import com.phatjam98.elasticsearch.utils.models.Field;
import com.phatjam98.geobuf.utils.GeobufUtils;
import com.phatjam98.protos.service.protos.SearchOperationType;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Creates a ShapeField to be used in searches.
 */
public class ShapeField extends Field {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShapeField.class);
  public static final String NOT_SUPPORTED_YET = "operationType: {} is not supported yet.";

  SearchOperationType operationType;

  public ShapeField(SearchOperationType operationType) {
    this.operationType = operationType;
  }

  @Override
  public Query queryBuilder() {
    GeoBufProtos.Data geobufData = (GeoBufProtos.Data) getValue().getValue();
    GeobufUtils geobufUtils = new GeobufUtils(geobufData);
    Query query = null;

    var geoShapeQueryBuilder = QueryBuilders.geoShape();
    geoShapeQueryBuilder.field(getName());
    GeoShapeFieldQuery.Builder shapeBuilder = new GeoShapeFieldQuery.Builder();
    shapeBuilder.shape(JsonData.fromJson(geobufUtils.getGeoJson()));

    var shapeFieldBuilder = new GeoShapeFieldQuery.Builder();
    shapeFieldBuilder.shape(JsonData.fromJson(geobufUtils.getGeoJson()));
    addOpType(shapeFieldBuilder);

    geoShapeQueryBuilder.shape(shapeFieldBuilder.build());

    if (isNested()) {
      query = nestedQuery(geoShapeQueryBuilder.build())._toQuery();
    } else {
      query = geoShapeQueryBuilder.build()._toQuery();
    }

    return query;
  }

  void addOpType(GeoShapeFieldQuery.Builder builder) {
    switch (operationType) {
      case GEO_CONTAINS:
        builder.relation(GeoShapeRelation.Within);
        break;
      case GEO_INTERSECTS:
        builder.relation(GeoShapeRelation.Intersects);
        break;
      case GEO_BOUNDING_BOX:
      case GEO_DISTANCE:
      case GEO_CONTAINED:
        LOGGER.error(NOT_SUPPORTED_YET, operationType);
        break;
      default:
        throw new UnsupportedOperationException(
            "The provided operationType is not supported.  Type: {}" + operationType);
    }
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
    ShapeField that = (ShapeField) o;
    return operationType == that.operationType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), operationType);
  }
}
