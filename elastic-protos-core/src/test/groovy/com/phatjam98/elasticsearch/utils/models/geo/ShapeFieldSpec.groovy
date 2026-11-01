package com.phatjam98.elasticsearch.utils.models.geo

import co.elastic.clients.elasticsearch._types.query_dsl.Query
import com.phatjam98.core.common.proto.FlatStructProtos
import com.phatjam98.core.common.proto.GeoBufProtos
import com.phatjam98.elasticsearch.utils.models.FieldValue
import com.phatjam98.geobuf.utils.GeobufUtils
import com.phatjam98.protos.service.protos.SearchOperationType
import org.elasticsearch.geometry.Line
import org.elasticsearch.geometry.MultiPoint
import org.elasticsearch.geometry.Point
import org.elasticsearch.geometry.Polygon
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class ShapeFieldSpec extends Specification {

    @Shared
    GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null)

    void setup() {
    }

    void cleanup() {
    }

    def "QueryBuilder"() {
    }

    def "GetGeoShapeQueryBuilder"() {
    }

    @Unroll
    def "GetEsGeometry #geoType"() {
        given:
        GeobufUtils geobufUtils = new GeobufUtils(geoJson)
        var shapeField = new ShapeField(SearchOperationType.GEO_DISTANCE)
                .setValue(new FieldValue().setValue(GeoBufProtos.Data.parseFrom(geobufUtils.getGeobuf()))
                        .setKind(FlatStructProtos.FlatValue.KindCase.GEO_VALUE))
                .setName("field_name")

        when:
        var result = shapeField.queryBuilder()

        then:
        result._kind() == Query.Kind.GeoShape

        where:
        geoJson          | geoType
        pointJson()      | Point
        multiPointJson() | MultiPoint
        lineStringJson() | Line
        polygonJson()    | Polygon
    }

    String pointJson() {
        return "{\"coordinates\": [-115.15, 36.10],\"type\":\"Point\"}"
    }

    String multiPointJson() {
        return "{\"coordinates\": [[-115.1,36.1],[-122.36,37.83]],\"type\": \"MultiPoint\"}"
    }

    String lineStringJson() {
        return "{\"coordinates\": [[-122.34949075320944,37.8741902042175],[-119.12945647128248,37.1629868888142],[-115.08926367212823,36.16947854823522]],\"type\": \"LineString\"}"
    }

    String polygonJson() {
        return "{\"coordinates\": [[[-115.30744907973501,36.15041709488544],[-115.30744907973501,36.054592480279894]," +
                "[-115.15702942251744,36.054592480279894],[-115.15702942251744,36.15041709488544]," +
                "[-115.30744907973501,36.15041709488544]]],\"type\": \"Polygon\"}"
    }
}
