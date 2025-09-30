package com.phatjam98.elasticsearch.utils.models

import co.elastic.clients.elasticsearch._types.query_dsl.Query
import com.phatjam98.core.common.proto.FlatStructProtos
import spock.lang.Specification
import spock.lang.Unroll

class ExistsFieldTest extends Specification {
    void setup() {
    }

    void cleanup() {
    }

    @Unroll
    def "QueryBuilder #fieldName #queryString"() {
        given:
        var existsField = new ExistsField().setName(fieldName);

        when:
        var result = existsField.queryBuilder()

        then:
        result._kind() == Query.Kind.Exists
        result.exists()._toQuery().toString() == queryString

        where:
        fieldName     | queryString
        "first_name"  | "Query: {\"exists\":{\"field\":\"first_name\"}}"
        "county_name" | "Query: {\"exists\":{\"field\":\"county_name\"}}"
        "nickname"    | "Query: {\"exists\":{\"field\":\"nickname\"}}"
    }
}
