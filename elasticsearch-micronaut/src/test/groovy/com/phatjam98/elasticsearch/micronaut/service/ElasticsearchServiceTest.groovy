package com.phatjam98.elasticsearch.micronaut.service

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient
import co.elastic.clients.elasticsearch._types.Result
import co.elastic.clients.elasticsearch._types.mapping.Property
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.SearchResponse
import co.elastic.clients.elasticsearch.indices.RefreshRequest
import co.elastic.clients.elasticsearch.indices.update_aliases.Action
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.ElasticsearchTransport
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.MapDifference
import com.google.common.collect.Maps
import com.google.protobuf.util.JsonFormat
import com.phatjam98.elasticsearch.utils.IndexUtils
import com.phatjam98.elasticsearch.utils.ResponseUtils
import com.phatjam98.helpers.TestLoggingHelpers
import com.thepublichealthco.protos.MappingTest
import groovy.json.JsonSlurper
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.testcontainers.elasticsearch.ElasticsearchContainer
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Instant

import static org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions

class ElasticsearchServiceTest extends Specification {
    private ListAppender<ILoggingEvent> listAppender

    @Shared
    ElasticsearchContainer container = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.15.2")

    @Shared
    ElasticsearchService service

    @Shared
    RestClient restClient

    @Shared
    ElasticsearchAsyncClient client

    void setupSpec() {
        container.start()
        restClient = RestClient.builder(HttpHost.create(container.getHttpHostAddress())).build()
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper())
        client = new ElasticsearchAsyncClient(transport)
        service = new ElasticsearchService(client)
    }

    void cleanupSpec() {
        container.stop()
    }

    void setup() {
        listAppender = TestLoggingHelpers.getListAppender(ElasticsearchService)
    }

    void cleanup() {
    }

    def "index Exists, Created, Deleted"() {
        given:
        var indexName = IndexUtils.getIndexName(MappingTest)

        expect:
        !service.indexExists(indexName)

        when:
        service.createIndex(indexName, IndexUtils.getTypeMapping(MappingTest))

        then:
        service.indexExists(indexName)

        when:
        service.deleteIndex(indexName)

        then:
        !service.indexExists(indexName)
    }

    @Unroll
    def "Create #userName"() {
        given:
        var indexName = IndexUtils.getIndexName(MappingTest)
        service.createIndex(indexName, IndexUtils.getTypeMapping(MappingTest))
        var testObj = MappingTest.newBuilder().setId(id).setInt64Value(createdAt).setStringValue(userName)
                .setEnumValue(MappingTest.Enum.ONE).build()
        var jsonDoc = JsonFormat.printer().omittingInsignificantWhitespace().preservingProtoFieldNames().print(testObj)

        when:
        var result = service.create(indexName, testObj.getId(), jsonDoc)
        service.refresh(new RefreshRequest.Builder().index(indexName).build())

        then:
        result.result() == Result.Created

        when:
        SearchResponse<HashMap> response =
                service.search(new SearchRequest.Builder().index(indexName).build(), HashMap)

        then:
        var searchHit = response.hits().hits().first()
        MappingTest testResponse = null
        MappingTest.Builder testResponseBuilder = MappingTest.newBuilder()

        if (searchHit != null) {
            testResponse = ResponseUtils.getBuilderFromHit(searchHit, testResponseBuilder).build()
        }

        if (testResponse != null) {
            testResponse.getId() == id
            testResponse.getInt64Value() == createdAt
            testResponse.getStringValue() == userName
            testResponse.getEnumValue() == MappingTest.Enum.ONE
        }

        cleanup:
        service.deleteIndex(indexName)

        where:
        id                           | createdAt                    | userName  | userType
        UUID.randomUUID().toString() | Instant.now().toEpochMilli() | "JohnBob" | 1
        UUID.randomUUID().toString() | Instant.now().toEpochMilli() | "BobJohn" | 2
    }

    def "Bulk"() {
    }

    def "BulkCreate"() {
    }

    def "Search"() {
    }

    def "Refresh"() {
    }

    @Unroll
    def "update"() {
        given:
        var indexName = IndexUtils.getIndexName(MappingTest)
        service.createIndex(indexName, IndexUtils.getTypeMapping(MappingTest))
        var testObj = MappingTest.newBuilder().setId(id).setInt64Value(createdAt).setStringValue(userName)
                .setEnumValue(MappingTest.Enum.ONE).build()
        var jsonDoc = JsonFormat.printer().omittingInsignificantWhitespace()
                .preservingProtoFieldNames().print(testObj)

        when:
        var result = service.create(indexName, testObj.getId(), jsonDoc)
        service.refresh(new RefreshRequest.Builder().index(indexName).build())

        then:
        result.result() == Result.Created

        when:
        var patchTestObj = MappingTest.newBuilder().setEnumValue(MappingTest.Enum.TWO).build()
        var patchJsonDoc = JsonFormat.printer().omittingInsignificantWhitespace()
                .preservingProtoFieldNames().print(patchTestObj)
        var patchMap = new ObjectMapper().readValue(patchJsonDoc, Map<String, Object>.class)
        var response = service.update(indexName, id, patchMap)

        then:
        response.result() == Result.Updated
        service.refresh(new RefreshRequest.Builder().index(indexName).build())
        var afterUpdate = service.search(new SearchRequest.Builder().index(indexName).build(), Map)
        var searchHit = afterUpdate.hits().hits().first()
        MappingTest mappingTestResponse = ResponseUtils.getBuilderFromHit(searchHit, MappingTest.newBuilder()).build()
        mappingTestResponse.getId() == id
        mappingTestResponse.getInt64Value() == createdAt
        mappingTestResponse.getStringValue() == userName
        mappingTestResponse.getEnumValue() == MappingTest.Enum.TWO

        cleanup:
        service.deleteIndex(indexName)

        where:
        id                           | createdAt                      | userName  | userType | updateUserType
        UUID.randomUUID().toString() | Instant.now().getEpochSecond() | "JohnBob" | 1        | 50
        UUID.randomUUID().toString() | Instant.now().getEpochSecond() | "BobJohn" | 2        | 100
    }

    @Unroll
    def "existingMappings #resource"() {
        given:
        var mappings = IndexUtils.getTypeMapping(resource)
        service.createIndex(indexName, mappings)
        service.updateAliases(indexName, aliasName, Action.Kind.Add)

        when:
        var response = service.existingMappings(resource)

        then:
        response.properties().keySet() == mappings.properties().keySet()
        for (String key : mappings.properties().keySet()) {
            response.properties().get(key)._kind() == mappings.properties().get(key)._kind()
        }

        cleanup:
        service.deleteIndex(indexName)

        where:
        resource    | indexName                         | aliasName
        MappingTest | IndexUtils.getIndexName(resource) | IndexUtils.getAlias(resource)

    }

    @Unroll
    def "updateMappings #resource"() {
        given:
        var mappings = IndexUtils.getTypeMapping(resource)
        var properties = new HashMap<>(mappings.properties())
        properties.remove(properties.keySet().first())
        if (service.indexExists(indexName)) {
            service.deleteIndex(indexName)
        }
        var initialMappings = new TypeMapping.Builder().properties(properties).build();
        service.createIndex(indexName, initialMappings)
        service.updateAliases(indexName, aliasName, Action.Kind.Add)
        var existingMappings = service.existingMappings(resource)

        expect:
        initialMappings.properties().keySet() == existingMappings.properties().keySet()
        for (String key : initialMappings.properties().keySet()) {
            initialMappings.properties().get(key)._kind() == existingMappings.properties().get(key)._kind()
        }

        when:
        var boolUpdate = service.updateMappings(resource)

        then:
        boolUpdate

        when:
        var newMappings = service.existingMappings(resource)

        then:
        newMappings.properties().keySet() == mappings.properties().keySet()
        for (String key : mappings.properties().keySet()) {
            newMappings.properties().get(key)._kind() == mappings.properties().get(key)._kind()
        }

        cleanup:
        service.deleteIndex(indexName)

        where:
        resource    | indexName                         | aliasName
        MappingTest | IndexUtils.getIndexName(resource) | IndexUtils.getAlias(resource)
    }

    @Unroll
    def "clusterSettings"() {
        when:
        var response = service.clusterHealth()

        then:
        response.clusterName() == "docker-cluster"
        response.numberOfDataNodes() == 1

    }

    @Unroll
    def "updateAliases #aliasName"() {
        given:
        for (String indexName : indexNames) {
            TypeMapping.Builder tm = new TypeMapping.Builder().properties(new HashMap<String, Property>() {})
            if (!service.indexExists(indexName)) {
                service.createIndex(indexName, tm.build())
            }
        }

        when:
        var response = service.updateAliases(indexNames, aliasName, Action.Kind.Add)

        then:
        response.acknowledged()
        service.indexExists(aliasName) == true

        when:
        var removeResponse = service.updateAliases(indexNames, aliasName, Action.Kind.Remove)

        then:
        response.acknowledged()
        service.indexExists(aliasName) == false

        where:
        version | indexNames                                | aliasName
        "v1"    | ["test_index_" + version] as List<String> | "test_index"
    }

    @Unroll
    def "bootstrapService #indexName"() {
        given:
        listAppender.stop()
        var indexName = IndexUtils.getIndexName(MappingTest)
        var aliasName = IndexUtils.getAlias(MappingTest)
        if (service.indexExists(indexName)) {
            service.deleteIndex(indexName)
        }
        !service.indexExists(indexName)
        listAppender.start()

        when:
        service.bootstrapService(MappingTest)

        then:
        service.indexExists(indexName)
        listAppender.list.get(0).toString().contains(aliasName + " exists: false")
        listAppender.list.get(1).toString().contains(indexName + " created: true")
        listAppender.list.get(2).toString().contains("Index created for " + indexName)
        listAppender.list.get(3).toString().contains("Mappings for index " + indexName + " set as:")
        listAppender.list.get(4).toString().contains("Aliases updated: UpdateAliasesResponse: {\"acknowledged\":true}")
        listAppender.list.get(5).toString().contains(indexName + " exists: true")

        when:
        service.bootstrapService(MappingTest)

        then:
        listAppender.list.get(6).toString().contains("Index " + aliasName + " exists: true")

        cleanup:
        service.deleteIndex(indexName)
    }

    @Unroll
    def "bootstrap update mappings #indexName"() {
        given:
        var oldMappings = new TypeMapping.Builder().properties(new HashMap<String, Property>() {}).build()

        service.createIndex(indexName, oldMappings)
        service.updateAliases(indexName, aliasName, Action.Kind.Add)
        service.indexExists(indexName)

        when:
        service.bootstrapService(resource)

        then:
        listAppender.list.get(0).toString().contains("[INFO] Index " + indexName +" created: true")

        cleanup:
        service.deleteIndex(indexName)

        where:
        resource    | indexName                         | aliasName                     | mappings                            | painlessLog
        MappingTest | IndexUtils.getIndexName(resource) | IndexUtils.getAlias(resource) | IndexUtils.getTypeMapping(resource) | "[INFO] Painless script found"
    }

    @Ignore("Revisit this to re-enable simple mapping updates without a reindex")
    @Unroll
    def "bootstrap failed mappings #indexName"() {
        given:
        var jsonStr = new File("src/test/resources/fixtures/bad_mappings.json").text
        var mappings = new JsonSlurper().parseText(jsonStr) as Map<String, Object>
//        service.createIndex(badIndexName, mappings)
//        service.indexExists(badIndexName)
//        service.updateAliases([badIndexName] as String[], aliasName, AliasActions.Type.ADD)

        when:
        service.bootstrapService(resource)

        then:
        listAppender.list.get(0).toString().contains("Mappings failed to update.")
        listAppender.list.get(1).toString().contains("Reindex completed successfully for " + indexName)
        listAppender.list.get(2).toString().contains("Index reindexed and Mappings match for " + indexName)

        cleanup:
        service.deleteIndex(indexName)

        where:
        resource    | indexName                         | badIndexName       | aliasName
        MappingTest | IndexUtils.getIndexName(resource) | indexName + "_bad" | IndexUtils.getAlias(resource)
    }

    @Unroll
    def "indexNamesFromAlias #aliasName"() {
        given:
        service.createIndex(indexName, mappings)
        service.updateAliases(indexName, aliasName, Action.Kind.Add)

        when:
        var result = service.indexNamesFromAlias(aliasName);

        then:
        result.size() == 1
        result[0] == indexName

        cleanup:
        service.deleteIndex(indexName)

        where:
        resource    | indexName                         | aliasName                     | mappings
        MappingTest | IndexUtils.getIndexName(resource) | IndexUtils.getAlias(resource) | IndexUtils.getTypeMapping(resource)
    }
}
