package com.phatjam98.elasticsearch.utils.models

import co.elastic.clients.elasticsearch._types.query_dsl.Query
import com.phatjam98.core.common.proto.FlatStructProtos
import spock.lang.Specification
import spock.lang.Unroll

class RangeFieldTest extends Specification {
    void setup() {
    }

    void cleanup() {
    }

    def "IsMinInclusive"() {
    }

    def "SetMinInclusive"() {
    }

    def "IsMaxInclusive"() {
    }

    def "SetMaxInclusive"() {
    }

    @Unroll
    def "QueryBuilder"() {
        given:
        var rangeField =
                new RangeField().setName(fieldName).setJsonPath("Rating").setValue(new FieldValue().setKind(
                        FlatStructProtos.FlatValue.KindCase.RANGE_VALUE).setValue(
                        FlatStructProtos.RangeValue.newBuilder().setMax(max).setMin(min).setMaxInclusive(maxInclusive)
                                .setMinInclusive(minInclusive).build()))

        when:
        var result = rangeField.queryBuilder()

        then:
        result._kind() == Query.Kind.Range
        result.range().field() == fieldName
        result.range().gte().toString() == Integer.toString(min)
        result.range().lt().toString() == Integer.toString(max)

        where:
        fieldName     | max | min | maxInclusive | minInclusive
        "numberField" | 95  | 35  | false        | true
    }

    def "Equals"() {
    }

    def "HashCode"() {
    }
}
