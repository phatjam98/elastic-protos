package com.phatjam98.elasticsearch.utils.models

import co.elastic.clients.elasticsearch._types.query_dsl.Query
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders
import spock.lang.Specification
import spock.lang.Unroll

import static com.phatjam98.core.common.proto.FlatStructProtos.FlatValue.KindCase

class FieldTest extends Specification {

    static class TempField extends Field {
        @Override
        Query queryBuilder() {
            return null
        }
    }

    void setup() {
    }

    void cleanup() {
    }

    def "QueryBuilder"() {
        given:
        TempField field = new TempField();

        when:
        var result = field.queryBuilder()

        then:
        result == null
    }

    @Unroll
    def "EsEscapeString #testValue"() {
        given:
        var field = new TempField()
        var value = testValue

        when:
        var result = field.esEscapeString(value)

        then:
        for (String esc : TempField.esReserved) {
            !result.contains(esc)
        }
        result.equals(expected)

        where:
        testValue                                                                                                | expected
        "some*thing"                                                                                             | "some thing"
        "some\\thing"                                                                                            | "some thing"
        "some-thing"                                                                                             | "some thing"
        "some=thing"                                                                                             | "some thing"
        "some&&thing"                                                                                            | "some thing"
        "some||thing"                                                                                            | "some thing"
        "some!thing"                                                                                             | "some thing"
        "some(thing"                                                                                             | "some thing"
        "some)thing"                                                                                             | "some thing"
        "some{thing"                                                                                             | "some thing"
        "some}thing"                                                                                             | "some thing"
        "some[thing"                                                                                             | "some thing"
        "some]thing"                                                                                             | "some thing"
        "some^thing"                                                                                             | "some thing"
        "some\"thing"                                                                                            | "some thing"
        "some?thing"                                                                                             | "some thing"
        "some:thing"                                                                                             | "some thing"
        "some/thing"                                                                                             | "some thing"
        "some\\thing*crazy-is=going&&on||here!fun(times)ahead{for}strings[and]stuff^and\"things?for:stuff/today" | "some thing crazy is going on here fun times ahead for strings and stuff and things for stuff today"
    }

    @Unroll
    def "NestedQuery #fieldName"() {
        given:
        var field = new TempField().setName(fieldName)
        var query = QueryBuilders.exists().field(fieldName).queryName(queryName).build()

        when:
        var result = field.nestedQuery(query)

        then:
        result._queryKind() == Query.Kind.Nested
        result.path() == path
        result.query()._kind() == Query.Kind.Exists

        where:
        fieldName    | queryName   | path
        "some_name" | "something" | fieldName
    }

    @Unroll
    def "IsNested #name #expected"() {
        given:
        var field = new TempField().setName(name)

        when:
        var result = field.isNested()

        then:
        result == expected

        where:
        name            | expected
        "location.city" | true
        "location"      | false
    }

    @Unroll
    def "ValueString #name"() {
        given:
        var fieldValue = new FieldValue().setKind(kind).setValue(value)
        var field = new TempField().setValue(fieldValue).setName(name)

        when:
        var result = field.valueString(fieldValue)

        then:
        result.equals(expected)

        where:
        kind                            | value             | name                  | expected
        KindCase.BOOL_VALUE             | true              | "bool_true"           | "true"
        KindCase.BOOL_VALUE             | false             | "bool_false"          | "false"
        KindCase.NUMBER_VALUE           | 10.52             | "number_10.52"        | "10.52"
        KindCase.NUMBER_VALUE           | 10                | "number_10"           | "10"
        KindCase.STRING_VALUE           | "string_value"    | "string_string_value" | "string_value"
        KindCase.GEO_BOUNDING_BOX_VALUE | "something crazy" | "unsupported"         | null

    }

    @Unroll
    def "Equals #testName"() {
        when:
        var fieldValue = new FieldValue().setKind(KindCase.STRING_VALUE).setValue("some_value")
        var compareFieldValue = new FieldValue().setKind(compareKind).setValue(compareValue)
        var field = new TempField().setName("some_field").setJsonPath("some_json_path").setValue(fieldValue)
        var compareField = new TempField().setName(compareFieldName).setJsonPath(compareJsonPath).setValue(compareFieldValue)

        then:
        (field == compareField) == expected

        where:
        testName          | compareFieldName | compareJsonPath   | compareKind           | compareValue  | expected
        "match"           | "some_field"     | "some_json_path"  | KindCase.STRING_VALUE | "some_value"  | true
        "diff field name" | "other_field"    | "some_json_path"  | KindCase.STRING_VALUE | "some_value"  | false
        "diff json path"  | "some_field"     | "other_json_path" | KindCase.STRING_VALUE | "some_value"  | false
        "diff kind"       | "some_field"     | "some_json_path"  | KindCase.BOOL_VALUE   | "some_value"  | false
        "diff value"      | "some_field"     | "some_json_path"  | KindCase.STRING_VALUE | "other_value" | false
    }

    @Unroll
    def "HashCode #testName"() {
        when:
        var fieldValue = new FieldValue().setKind(KindCase.STRING_VALUE).setValue("some_value")
        var compareFieldValue = new FieldValue().setKind(compareKind).setValue(compareValue)
        var field = new TempField().setName("some_field").setJsonPath("some_json_path").setValue(fieldValue)
        var compareField = new TempField().setName(compareFieldName).setJsonPath(compareJsonPath).setValue(compareFieldValue)

        then:
        (field.hashCode() == compareField.hashCode()) == expected

        where:
        testName          | compareFieldName | compareJsonPath   | compareKind           | compareValue  | expected
        "match"           | "some_field"     | "some_json_path"  | KindCase.STRING_VALUE | "some_value"  | true
        "diff field name" | "other_field"    | "some_json_path"  | KindCase.STRING_VALUE | "some_value"  | false
        "diff json path"  | "some_field"     | "other_json_path" | KindCase.STRING_VALUE | "some_value"  | false
        "diff kind"       | "some_field"     | "some_json_path"  | KindCase.BOOL_VALUE   | "some_value"  | false
        "diff value"      | "some_field"     | "some_json_path"  | KindCase.STRING_VALUE | "other_value" | false
    }
}
