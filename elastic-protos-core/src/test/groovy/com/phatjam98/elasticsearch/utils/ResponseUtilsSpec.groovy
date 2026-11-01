package com.phatjam98.elasticsearch.utils

import co.elastic.clients.elasticsearch.core.SearchResponse
import co.elastic.clients.elasticsearch.core.search.Hit
import co.elastic.clients.elasticsearch.core.search.HitsMetadata
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.util.JsonFormat
import com.phatjam98.protos.service.protos.Pagination
import com.phatjam98.protos.service.protos.SearchCriteria
import org.apache.lucene.search.TotalHits
import org.elasticsearch.search.aggregations.Aggregations
import org.elasticsearch.search.profile.SearchProfileResults
import org.elasticsearch.search.suggest.Suggest
import spock.lang.Specification
import spock.lang.Unroll

class ResponseUtilsSpec extends Specification {
    void setup() {
    }

    void cleanup() {
    }

    @Unroll
    def "GetPagination #name"() {
        given:
        SearchCriteria searchCriteria = createSearchCriteria(from)
        SearchResponse searchResponse = createSearchResponse(from, totalHits, totalReturned, scrollId, pit)

        when:
        Pagination pagination = ResponseUtils.getPagination(searchCriteria, searchResponse)

        then:
        pagination.getFrom() == from
        pagination.getTotal() == totalHits
        pagination.getCursor() == pit
        pagination.getSize() == totalReturned

        where:
        from | totalHits | totalReturned | scrollId | pit       | name
        0    | 100       | 10            | ""       | ""        | "0 100 10"
        10   | 100       | 10            | ""       | ""        | "10 100 10"
        0    | 100       | 10            | ""       | "somePit" | "0 100 10 somePit"
        0    | 100       | 10            | "scroll" | ""        | "0 100 10 scroll"
    }

    @Unroll
    def "GetPagination #name"() {
        given:
        SearchCriteria searchCriteria = createSearchCriteria(from)
        SearchResponse searchResponse = createSearchResponse(Collections.emptyList())

        when:
        Pagination pagination = ResponseUtils.getPagination(searchCriteria, searchResponse)

        then:
        pagination.getFrom() == from
        pagination.getTotal() == totalHits
        pagination.getCursor() == pit
        pagination.getSize() == totalReturned

        where:
        from | totalHits | totalReturned | scrollId | pit       | name
        0    | 0         | 0             | ""       | ""        | "empty"
    }

    @Unroll
    def "GetBuilderFromHit #docId"() {
        given:
        Pagination pagination = Pagination.newBuilder().setFrom(from).setSize(size).setTotal(total).setCursor(cursor).build()
        String jsonStr = JsonFormat.printer().preservingProtoFieldNames().omittingInsignificantWhitespace()
                .print(pagination)
        Map<String, Object> source = new ObjectMapper().readValue(jsonStr, Map<String, Object>.class)
        Hit hit = createSearchHit(docId, source)

        when:
        var builder = ResponseUtils.<Pagination.Builder> getBuilderFromHit(hit, Pagination.newBuilder())
        var proto = builder.build()

        then:
        proto.class == Pagination.class
        Pagination result = Pagination.newBuilder((Pagination) proto).build()
        result.getFrom() == from
        result.getSize() == size
        result.getTotal() == total
        result.getCursor() == cursor

        where:
        docId | from | size | total | cursor
        "1"     | 0    | 10   | 100   | "cursor1"
        "2"     | 10   | 10   | 100   | "cursor2"
        "3"     | 90   | 30   | 1000  | "cursor3"
    }

    SearchCriteria createSearchCriteria(int from) {
        return SearchCriteria.newBuilder()
                        .setPagination(Pagination.newBuilder()
                                .setFrom(from)).build()
    }

    SearchResponse createSearchResponse(int from, int totalHitsValue, int totalReturned, String scrollId, String pointInTimeId) {
        scrollId = scrollId.isBlank() ? null : scrollId
        pointInTimeId = pointInTimeId.isBlank() ? null : pointInTimeId
        SearchResponse<Map<String, Object>> searchResponseBuilder = null

        if (totalHitsValue == 0) {
            searchResponseBuilder = createSearchResponse(null)
        } else {
            List<Hit> searchHitsHits = generateSearchHits(totalReturned)
            searchResponseBuilder = createSearchResponse(searchHitsHits, from, totalHitsValue, scrollId, pointInTimeId)
        }

        return searchResponseBuilder
    }

    List<Hit> generateSearchHits(int totalReturned) {
        List<Hit> searchHitsHits = new ArrayList<Hit>()
        for (int i = 0; i < totalReturned; i++) {
            searchHitsHits.add(createSearchHit(String.valueOf(i), null))
        }
        return searchHitsHits
    }

    SearchResponse<Map<String, Object>> createSearchResponse(List<Hit> hits) {
        var responseBody = new SearchResponse.Builder<Map<String, Object>>()
                .hits(hmd -> hmd.hits(hits)
                        .total(th -> th.relation(TotalHitsRelation.Eq).value(hits.size())))
                .took(25)
                .timedOut(false)
                .shards(s -> s.total(1).successful(1).skipped(0).failed(0))
                .build();

        return responseBody;
    }

    SearchResponse<Map<String, Object>> createSearchResponse(List<Hit> hits, int from, int totalHitsValue,
                                                             String scrollId, String pointInTimeId) {
        var responseBody = new SearchResponse.Builder<Map<String, Object>>()
                .hits(hmd -> hmd.hits(hits)
                        .total(th -> th.relation(TotalHitsRelation.Eq).value(totalHitsValue)))
                .took(25)
                .timedOut(false)
                .shards(s -> s.total(1).successful(1).skipped(0).failed(0))

        if (pointInTimeId != null) {
            responseBody.pitId(pointInTimeId)
        }

        if (scrollId != null) {
            responseBody.scrollId(scrollId)
        }

        return responseBody.build();
    }

    Hit<Map<String, Object>> createSearchHit(String id, Map<String, Object> source) {
        return new Hit.Builder<Map<String, Object>>().source(source).id(id).index("stuff").build()
    }
}


