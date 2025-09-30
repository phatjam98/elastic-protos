package com.phatjam98.elasticsearch.utils.models

import spock.lang.Specification
import spock.lang.Unroll

import static com.phatjam98.core.common.proto.FlatStructProtos.FlatValue.KindCase

class FieldValueTest extends Specification {
    void setup() {
    }

    void cleanup() {
    }

    def "GetKind"() {
    }

    def "SetKind"() {
    }

    def "GetValue"() {
    }

    def "SetValue"() {
    }

    @Unroll
    def "Equals #value"() {
        when:
        var fieldValue = new FieldValue().setValue(value).setKind(kind)
        var compareFieldValue = new FieldValue().setValue(compareValue).setKind(compareKind)

        then:
        fieldValue.equals(compareFieldValue) == expected

        where:
        value        | kind                  | compareValue | compareKind           | expected
        "matching"   | KindCase.STRING_VALUE | "matching"   | KindCase.STRING_VALUE | true
        "wrongKind"  | KindCase.STRING_VALUE | "wrongKind"  | KindCase.BOOL_VALUE   | false
        "wrongValue" | KindCase.STRING_VALUE | "helloWorld" | KindCase.STRING_VALUE | false
    }

    def "HashCode #value"() {
        when:
        var fieldValue = new FieldValue().setValue(value).setKind(kind)
        var compareFieldValue = new FieldValue().setValue(compareValue).setKind(compareKind)

        then:
        fieldValue.equals(compareFieldValue) == expected
        (fieldValue.hashCode() == compareFieldValue.hashCode()) == expected

        where:
        value        | kind                  | compareValue | compareKind           | expected
        "matching"   | KindCase.STRING_VALUE | "matching"   | KindCase.STRING_VALUE | true
        "wrongKind"  | KindCase.STRING_VALUE | "wrongKind"  | KindCase.BOOL_VALUE   | false
        "wrongValue" | KindCase.STRING_VALUE | "helloWorld" | KindCase.STRING_VALUE | false
    }
}
