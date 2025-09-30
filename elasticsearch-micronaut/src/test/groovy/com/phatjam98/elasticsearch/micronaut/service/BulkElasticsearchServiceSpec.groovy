package com.phatjam98.elasticsearch.micronaut.service

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation
import co.elastic.clients.elasticsearch.indices.RefreshRequest
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.ElasticsearchTransport
import co.elastic.clients.transport.rest_client.RestClientTransport
import co.elastic.clients.util.BinaryData
import co.elastic.clients.util.ContentType
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.util.JsonFormat
import com.phatjam98.elasticsearch.utils.IndexUtils
import com.phatjam98.elasticsearch.utils.RequestBuilder
import com.phatjam98.helpers.TestLoggingHelpers
import com.thepublichealthco.protos.MappingTest
import com.thepublichealthco.protos.service.protos.SearchCriteria
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.testcontainers.elasticsearch.ElasticsearchContainer
import spock.lang.Shared
import spock.lang.Specification

class BulkElasticsearchServiceSpec extends Specification {
    private ListAppender<ILoggingEvent> listAppender

    @Shared
    ElasticsearchContainer container = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.15.2")

    @Shared
    BulkElasticsearchService service

    @Shared
    RestClient restClient

    @Shared
    ElasticsearchAsyncClient client

    void setupSpec() {
        container.start()
        restClient = RestClient.builder(HttpHost.create(container.getHttpHostAddress())).build()
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper())
        client = new ElasticsearchAsyncClient(transport)
        service = new BulkElasticsearchService(client, 1, 1, 1)
    }

    void cleanupSpec() {
        container.stop()
    }

    void setup() {
        listAppender = TestLoggingHelpers.getListAppender(ElasticsearchService)
        service.bootstrapService(MappingTest)
    }

    void cleanup() {
        var indexName = IndexUtils.getIndexName(MappingTest.class)
        service.deleteIndex(indexName)
    }

    def "Bulk"() {
    }

    def "BulkCreate"() {
    }

    def "getBulkProcessor"() {
        given:
        var testProto = MappingTest.newBuilder().setStringValue("test").build()
        var jsonStr = JsonFormat.printer().omittingInsignificantWhitespace()
                .preservingProtoFieldNames().print(testProto)

        when:
        var bulkProcessor = service.getBulkProcessor()
        var indexName = IndexUtils.getIndexName(MappingTest.class)
        var dataMap = new ObjectMapper().readValue(jsonStr, Map<String, Object>.class)
        var bulkOp = new BulkOperation.Builder().index(op -> op.index(indexName)
                .id("1").document(dataMap)).build()
        bulkProcessor.add(bulkOp)
        bulkProcessor.flush()
        service.refresh(new RefreshRequest.Builder().index(indexName).build())

        then:
        Thread.sleep(1000)
        var thingy = service.search(new RequestBuilder().buildRequest(indexName, SearchCriteria.newBuilder().build()), Map.class)
        thingy.hits().hits().size() == 1
    }
}
