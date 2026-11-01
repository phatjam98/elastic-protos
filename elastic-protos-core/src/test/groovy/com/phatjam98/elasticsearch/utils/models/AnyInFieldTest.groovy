package com.phatjam98.elasticsearch.utils.models

import co.elastic.clients.elasticsearch._types.query_dsl.Query
import com.phatjam98.core.common.proto.FlatStructProtos
import spock.lang.Specification
import spock.lang.Unroll

import java.util.stream.Collectors

class AnyInFieldTest extends Specification {
    void setup() {
    }

    void cleanup() {
    }

    @Unroll
    def "QueryBuilder names #name1 #name2 #name3"() {
        given:
        var valuesList = new ArrayList<>();
        var value1 = FlatStructProtos.PrimitiveValue.newBuilder()
                .setStringValue(name1).build()
        var value2 = FlatStructProtos.PrimitiveValue.newBuilder()
                .setStringValue(name2).build()
        var value3 = FlatStructProtos.PrimitiveValue.newBuilder()
                .setStringValue(name3).build()
        valuesList.add(value1)
        valuesList.add(value2)
        valuesList.add(value3)

        var primitiveList = FlatStructProtos.PrimitiveList.newBuilder()
                .addAllValues(valuesList).build();
        var fieldValue = new FieldValue().setValue(primitiveList).setKind(
                FlatStructProtos.FlatValue.KindCase.KIND_NOT_SET);
        var anyInField =
                new AnyInField().setName("first_name").setValue(fieldValue).setJsonPath("some_path");

        when:
        var result = anyInField.queryBuilder()

        then:
        result._kind() == Query.Kind.Terms
        result.terms().terms().value().stream()
                .map(fv -> fv.stringValue())
                .collect(Collectors.toList()).containsAll([name1, name2, name3])

        where:
        name1   | name2  | name3
        "steve" | "sara" | "bob"
        "jill"  | "jack" | "nash"
    }
}
