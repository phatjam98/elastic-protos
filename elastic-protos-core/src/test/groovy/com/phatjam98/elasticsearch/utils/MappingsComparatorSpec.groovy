package com.phatjam98.elasticsearch.utils

import co.elastic.clients.elasticsearch._types.mapping.Property
import co.elastic.clients.elasticsearch._types.mapping.PropertyBuilders
import spock.lang.Specification

class MappingsComparatorSpec extends Specification {


    def "CompareMappings"() {
        given:
        Map<String, Property> nestedExistingMappings = [
                "innerKey1": PropertyBuilders.keyword(knp -> knp.index(true)),
                "innerKey2": PropertyBuilders.geoShape(gsp -> gsp.coerce(true))
        ]
        Map<String, Property> nestedNewMappings = [
                "innerKey1": PropertyBuilders.keyword(knp -> knp.index(true)),
                "innerKey2": PropertyBuilders.geoShape(gsp -> gsp.coerce(true))
        ]
        Map<String, Property> existingMappings = [
                "key1": PropertyBuilders.text(tnp -> tnp.index(true)),
                "key2": PropertyBuilders.boolean_(bnp -> bnp.index(true)),
                "key3": PropertyBuilders.nested(np -> np.properties(nestedExistingMappings))
        ]
        Map<String, Property> newMappings = [
                "key1": PropertyBuilders.text(tnp -> tnp.index(true)),
                "key2": PropertyBuilders.boolean_(bnp -> bnp.index(true)),
                "key3": PropertyBuilders.nested(np -> np.properties(nestedNewMappings))
        ]

        when:
        var result = MappingsComparator.compareMappings(existingMappings, newMappings, "", true)

        then:
        result

        when:
        existingMappings.put("key1", PropertyBuilders.boolean_(bnp -> bnp.index(true)))
        result = MappingsComparator.compareMappings(existingMappings, newMappings, "", true)

        then:
        !result

        when:
        existingMappings.put("key1", PropertyBuilders.text(tnp -> tnp.index(true)))
        existingMappings.put("newKey", PropertyBuilders.text(tnp -> tnp.index(true)))
        result = MappingsComparator.compareMappings(existingMappings, newMappings, "", true)

        then:
        !result

        when:
        nestedExistingMappings.put("innerKey1", PropertyBuilders.boolean_(bnp -> bnp.index(true)))
        var updateExisting = [
                "key1": PropertyBuilders.text(tnp -> tnp.index(true)),
                "key2": PropertyBuilders.boolean_(bnp -> bnp.index(true)),
                "key3": PropertyBuilders.nested(np -> np.properties(nestedExistingMappings))
        ]
        result = MappingsComparator.compareMappings(existingMappings, newMappings, "", true)

        then:
        !result

        when:
        newMappings = existingMappings
        result = MappingsComparator.compareMappings(existingMappings, newMappings, "", true)

        then:
        result

    }
}
