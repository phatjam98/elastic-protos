package com.phatjam98.elasticsearch.utils.models

import co.elastic.clients.elasticsearch._types.query_dsl.Query
import com.phatjam98.core.common.proto.FlatStructProtos.FlatValue.KindCase
import spock.lang.Specification
import spock.lang.Unroll

class ContainsFieldTest extends Specification {
    void setup() {
    }

    void cleanup() {
    }

    @Unroll
    def "QueryBuilder #fieldName #queryString"() {
        given:
        var containsField = new ContainsField()
        containsField.setName(fieldName)
        containsField.setJsonPath("jsonPath")
        containsField.setValue(new FieldValue().setKind(KindCase.STRING_VALUE).setValue(queryString))

        when:
        var result = containsField.queryBuilder()

        then:
        result._kind() == Query.Kind.QueryString
        result.queryString().query() == queryString

        where:
        fieldName     | queryString
        "first_name"  | "steve"
        "county_name" | "clark"
        "nickname"    | "sparky"
    }
}
