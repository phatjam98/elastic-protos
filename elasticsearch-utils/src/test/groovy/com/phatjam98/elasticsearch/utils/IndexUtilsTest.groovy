package com.phatjam98.elasticsearch.utils

import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping
import co.elastic.clients.elasticsearch._types.mapping.Property
import com.phatjam98.protos.MappingTest
import spock.lang.Specification
import spock.lang.Unroll

class IndexUtilsTest extends Specification {

    void setup() {
    }

    void cleanup() {
    }

    @Unroll
    def "GenerateMappings for #propertyChecks"() {
        given:
        var indexBuilder = new IndexUtils(MappingTest.class)

        when:
        var mappings = indexBuilder.generateTypeMapping()

        then:
        mappings.dynamic() == DynamicMapping.Strict
        var properties = (HashMap<String, Property>) mappings.properties()
        properties.containsKey("double_value")
        properties.get("double_value")._kind() == Property.Kind.Double
        properties.containsKey("float_value")
        properties.get("float_value")._kind() == Property.Kind.Float
        properties.containsKey("int64_value")
        properties.get("int64_value")._kind() == Property.Kind.Long
        properties.containsKey("int32_value")
        properties.get("int32_value")._kind() == Property.Kind.Integer
        properties.containsKey("bool_value")
        properties.get("bool_value")._kind() == Property.Kind.Boolean
        properties.containsKey("string_value")
        properties.get("string_value")._kind() == Property.Kind.Keyword
        properties.containsKey("uint64_value")
        properties.get("uint64_value")._kind() == Property.Kind.Long
        properties.containsKey("fixed64_value")
        properties.get("fixed64_value")._kind() == Property.Kind.Long
        properties.containsKey("sfixed64_value")
        properties.get("sfixed64_value")._kind() == Property.Kind.Long
        properties.containsKey("sint64_value")
        properties.get("sint64_value")._kind() == Property.Kind.Long
        properties.containsKey("uint32_value")
        properties.get("uint32_value")._kind() == Property.Kind.Integer
        properties.containsKey("fixed32_value")
        properties.get("fixed32_value")._kind() == Property.Kind.Integer
        properties.containsKey("sfixed32_value")
        properties.get("sfixed32_value")._kind() == Property.Kind.Integer
        properties.containsKey("sint32_value")
        properties.get("sint32_value")._kind() == Property.Kind.Integer
        properties.containsKey("bytes_value")
        properties.get("bytes_value")._kind() == Property.Kind.Text
        properties.containsKey("enum_value")
        properties.get("enum_value")._kind() == Property.Kind.Keyword
        properties.containsKey("nested_value")
        properties.get("nested_value")._kind() == Property.Kind.Nested
        properties.get("nested_value").nested().properties().containsKey("nested_string_value")
        properties.get("nested_value").nested().properties().get("nested_string_value")._kind() == Property.Kind.Keyword
    }

    def "extractIndexName for #klass"() {
        given:
        var indexBuilder = new IndexUtils(MappingTest)

        when:
        var indexName = indexBuilder.extractIndexName()

        then:
        indexName == "mapping_test-" + Math.abs(IndexUtils.getTypeMapping(MappingTest).properties().keySet().hashCode())
    }

    def "getIndexUtils"() {
        when:
        var indexUtils = IndexUtils.getIndexUtils(MappingTest)

        then:
        indexUtils.extractIndexName() == "mapping_test-" + Math.abs(IndexUtils.getTypeMapping(MappingTest).properties().keySet().hashCode())
    }

    def "getMappings"() {
        when:
        var mappings = IndexUtils.getTypeMapping(MappingTest)

        then:
        Map<String, Object> properties = mappings.properties()
        properties.get("double_value") != null
    }

    def "getIndexName"() {
        when:
        var name = IndexUtils.getIndexName(MappingTest)

        then:
        name == "mapping_test-" + Math.abs(IndexUtils.getTypeMapping(MappingTest).properties().keySet().hashCode())
    }

    def "getAlias"() {
        when:
        var alias = IndexUtils.getAlias(MappingTest)

        then:
        alias == "mapping_test"

        when:
        var utils = new IndexUtils(MappingTest)
        alias = utils.getAlias()

        then:
        alias == "mapping_test"
    }
}
