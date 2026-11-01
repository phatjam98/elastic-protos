package com.phatjam98.elasticsearch.utils.models

import co.elastic.clients.elasticsearch._types.query_dsl.Operator
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import com.phatjam98.core.common.proto.FlatStructProtos
import spock.lang.Specification
import spock.lang.Unroll

class MatchesFieldTest extends Specification {
    void setup() {
    }

    void cleanup() {
    }

    @Unroll
    def "QueryBuilder #fieldName #queryString"() {
        given:
        var matchesField = new MatchesField()
        matchesField.setName(fieldName)
        matchesField.setJsonPath("jsonPath")
        matchesField.setValue(new FieldValue().setKind(FlatStructProtos.FlatValue.KindCase.STRING_VALUE)
                .setValue(queryString))

        when:
        var result = matchesField.queryBuilder()

        then:
        result._kind() == Query.Kind.QueryString
        result.queryString().query() == queryString + "*"
        result.queryString().defaultOperator() == Operator.And

        where:
        fieldName     | queryString
        "first_name"  | "steve"
        "county_name" | "clark"
        "nickname"    | "sparky"
    }
}
