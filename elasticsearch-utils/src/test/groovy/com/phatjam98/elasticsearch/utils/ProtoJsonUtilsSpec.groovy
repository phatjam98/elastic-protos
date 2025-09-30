package com.phatjam98.elasticsearch.utils

import com.google.protobuf.util.Timestamps
import com.phatjam98.core.common.proto.GeoBufProtos
import com.phatjam98.core.common.proto.RiskProtos
import com.phatjam98.geobuf.utils.GeobufUtils
import com.phatjam98.protos.core.protos.AdminBoundary
import com.phatjam98.protos.core.protos.BaseEvent
import com.phatjam98.protos.core.protos.Bucket
import com.phatjam98.protos.core.protos.Confidence
import com.phatjam98.protos.v1.core.DiseaseThreat
import com.phatjam98.protos.v1.core.EnvironmentalThreat
import com.phatjam98.protos.v1.core.RiskIndex
import com.phatjam98.protos.v1.core.Threat
import com.phatjam98.protos.v1.core.ThreatGroup
import com.phatjam98.protos.v1.core.ThreatIndicator
import spock.lang.Specification

import java.text.SimpleDateFormat

class ProtoJsonUtilsSpec extends Specification {
    static final String GEOBUF_SHAPE_PATH = "src/test/resources/fixtures/geobuf_shape.json"
    static final String BBOX_SHAPE_PATH = "src/test/resources/fixtures/bbox_shape.json"
    static final String POINT_SHAPE_PATH = "src/test/resources/fixtures/point_shape.json"
    static final String HIT_PATH = "src/test/resources/fixtures/hit.json"
    static final String ADMIN_BOUNDARY_PATH = "src/test/resources/fixtures/admin_boundary.json"
    static final String GEOJSON_SAMPLE = "{\"coordinates\": [-115.28427619379418,36.076495086373384],\"type\": \"Point\"}"

    def "should convert RiskIndex to JSON with correct GeoJSON fields"() {
        given:
        var bucket = getBucket()

        when:
        var result = ProtoJsonUtils.getJsonFromProto(bucket)

        then:
        result instanceof String
        result.contains("\"coordinates\":[-115.284276,36.076495]")
        result.contains("\"type\":\"Point\"")

        when:
        var riskIndex = getRiskIndex()
        var riskIndexResult = ProtoJsonUtils.getJsonFromProto(riskIndex)

        then:
        riskIndexResult instanceof String
        riskIndexResult.contains("\"type\":\"Point\"")
        riskIndexResult.contains("\"coordinates\":[-115.284276,36.076495]")
    }

    def "should convert AdminBoundary to JSON with correct GeoJSON fields"() {
        given:
        var adminBoundary = createAdminBoundary()

        when:
        var result = ProtoJsonUtils.getJsonFromProto(adminBoundary)

        then:
        result instanceof String
        result.contains("\"type\":\"Polygon\"")
        result.contains("\"type\":\"Point\"")
    }

    def "should convert JSON to Proto with correct GeoBuf fields"() {
        given:
        var jsonStr = new File(HIT_PATH).text

        when:
        RiskIndex riskIndex = ProtoJsonUtils.convertJsonToProto(jsonStr, RiskIndex.class)

        then:
        riskIndex.getBaseEvent().getConfidence().getScore() == 0.7 as Float
        riskIndex.getThreatGroups(0).getThreatGroupType() == RiskProtos.ThreatGroupType.THREAT_GROUP_TYPE_DISEASE
        riskIndex.getThreatGroups(0).getTrend() == RiskProtos.Risk.Trend.TREND_INCREASING

        when:
        var geoJson = new GeobufUtils(riskIndex.getBaseEvent().getGeo()).getGeoJson()

        then:
        geoJson.contains('{"type":"Point","coordinates":[-115.284276,36.076495]}')
    }

    def "should convert JSON to Proto with correct Timestamp fields"() {
        given:
        var jsonStr = new File(ADMIN_BOUNDARY_PATH).text

        when:
        AdminBoundary adminBoundary = ProtoJsonUtils.convertJsonToProto(jsonStr, AdminBoundary.class)

        then:
        adminBoundary.getBaseEvent().getCreatedAt().getSeconds() > 0

        when:
        var geoJson = new GeobufUtils(riskIndex.getBaseEvent().getGeo()).getGeoJson()

        then:
        geoJson.contains('{"type":"Point","coordinates":[-115.284276,36.076495]}')
    }

    def createAdminBoundary() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
        var createdAt = formatter.parse("2023-10-02T19:33:32.819206")
        return AdminBoundary.newBuilder()
                .setBaseEvent(BaseEvent.newBuilder()
                        .setGeo(getCentroid())
                        .setLocationId("location1")
                        .setCreatedAt(Timestamps.fromDate(createdAt)))
                .setGeobuf(getGeoBuf())
                .setBbox(getBbox())
                .setCentroid(getCentroid())
                .setPlusCode("some plus code")
                .setPlaceKey("some place key")
                .setGeoHash("9qqj25")
                .setH3Cell("8a2986b13657fff")
                .setOsmId("osm1")
                .setLevel(1)
                .build()
    }

    GeoBufProtos.Data getGeoBuf() {
        var str = new File(GEOBUF_SHAPE_PATH).text

        return createGeoBuf(str)
    }

    GeoBufProtos.Data getBbox() {
        var str = new File(BBOX_SHAPE_PATH).text

        return createGeoBuf(str)
    }

    GeoBufProtos.Data getCentroid() {
        var str = new File(POINT_SHAPE_PATH).text

        return createGeoBuf(str)
    }

    GeoBufProtos.Data createGeoBuf(String geoJson) {
        var geobufUtils = new GeobufUtils(geoJson)

        return GeoBufProtos.Data.parseFrom(geobufUtils.getGeobuf())
    }

    BaseEvent.Builder getBaseEventBuilder() {
        var geobufUtils = new GeobufUtils(GEOJSON_SAMPLE)
        var data = GeoBufProtos.Data.parseFrom(geobufUtils.getGeobuf())
        return BaseEvent.newBuilder().setGeo(data)
    }

    Bucket getBucket() {
        return Bucket.newBuilder()
                .setBaseEvent(getBaseEventBuilder().setConfidence(Confidence.newBuilder().setScore(0.7)))
                .setDisease(RiskProtos.Disease.Type.TYPE_FLU)
                .setDiseaseIndicator(RiskProtos.Disease.Indicator.INDICATOR_CASES)
                .build()
    }

    RiskIndex getRiskIndex() {
        return RiskIndex.newBuilder()
                .setBaseEvent(getBaseEventBuilder().setConfidence(Confidence.newBuilder().setScore(0.7)))
                .addThreatGroups(ThreatGroup.newBuilder()
                        .setThreatGroupType(RiskProtos.ThreatGroupType.THREAT_GROUP_TYPE_DISEASE)
                        .setTrend(RiskProtos.Risk.Trend.TREND_INCREASING)
                        .addThreats(
                                Threat.newBuilder()
                                        .setDiseaseThreat(DiseaseThreat.newBuilder()
                                                .setDiseaseType(RiskProtos.Disease.Type.TYPE_FLU))))
                .addThreatGroups(ThreatGroup.newBuilder().setThreatGroupType(RiskProtos.ThreatGroupType.THREAT_GROUP_TYPE_ENVIRONMENT)
                        .setTrend(RiskProtos.Risk.Trend.TREND_DECREASING)
                        .addThreats(Threat.newBuilder().setEnvirontmentalThreat(EnvironmentalThreat.newBuilder()
                                .addThreatIndicators(ThreatIndicator.newBuilder()
                                        .addIndicatorText("some indicator")))))
                .build()
    }
}
