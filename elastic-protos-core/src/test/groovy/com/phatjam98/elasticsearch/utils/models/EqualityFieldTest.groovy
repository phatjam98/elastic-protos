package com.phatjam98.elasticsearch.utils.models

import co.elastic.clients.elasticsearch._types.query_dsl.Query
import com.phatjam98.core.common.proto.FlatStructProtos
import spock.lang.Specification
import spock.lang.Unroll

class EqualityFieldTest extends Specification {
    void setup() {
    }

    void cleanup() {
    }

    @Unroll
    def "QueryBuilder #fieldName #queryString"() {
        given:
        var equalityField = new EqualityField().setName(fieldName).setJsonPath("jsonPath")
                .setValue(new FieldValue().setKind(FlatStructProtos.FlatValue.KindCase.STRING_VALUE).setValue(queryString))

        when:
        var result = equalityField.queryBuilder()

        then:
        result._kind() == Query.Kind.Term
        result.term().value().stringValue() == queryString

        where:
        fieldName     | queryString
        "first_name"  | "steve"
        "county_name" | "clark"
        "nickname"    | "sparky"
    }
}
