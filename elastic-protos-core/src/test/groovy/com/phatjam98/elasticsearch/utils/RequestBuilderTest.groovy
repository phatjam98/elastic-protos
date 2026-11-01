package com.phatjam98.elasticsearch.utils

import co.elastic.clients.elasticsearch._types.SortOptions
import co.elastic.clients.elasticsearch._types.SortOrder
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import co.elastic.clients.elasticsearch.core.SearchRequest
import com.phatjam98.core.common.proto.FlatStructProtos
import com.phatjam98.core.common.proto.FlatStructProtos.DistanceType
import com.phatjam98.protos.service.protos.DistanceUnit
import com.phatjam98.protos.service.protos.FieldCondition
import com.phatjam98.protos.service.protos.FieldSort
import com.phatjam98.protos.service.protos.GeoDistanceSort
import com.phatjam98.protos.service.protos.Pagination
import com.phatjam98.protos.service.protos.ScoreSort
import com.phatjam98.protos.service.protos.ScriptSort
import com.phatjam98.protos.service.protos.ScriptSortType
import com.phatjam98.protos.service.protos.SearchCondition
import com.phatjam98.protos.service.protos.SearchCriteria
import com.phatjam98.protos.service.protos.SearchOperationType
import com.phatjam98.protos.service.protos.SortingCriteria
import com.phatjam98.protos.service.protos.SortingOrder
import spock.lang.Specification
import spock.lang.Unroll

class RequestBuilderTest extends Specification {

    void setup() {
    }

    void cleanup() {
    }

    //TODO: This is focused on the sorts.  I need to finish adding all these tests back in.
    def "BuildRequest"() {
        given:
        RequestBuilder requestBuilder = new RequestBuilder()

        FlatStructProtos.FlatValue flatValue = FlatStructProtos.FlatValue.newBuilder()
                .setStringValue("John")
                .build()
        FieldCondition fieldCondition = FieldCondition.newBuilder()
                .setOperation(SearchOperationType.EQUALS)
                .setField("name")
                .setValue(flatValue)
                .build()
        SearchCondition searchCondition = SearchCondition.newBuilder()
                .addFieldCondition(fieldCondition)
                .build()
        SortingCriteria sortingCriteria = SortingCriteria.newBuilder().setFieldSort(
                FieldSort.newBuilder().setFieldName(sortFieldName).setOrder(sortDirection).build()).build()
        Pagination pagination = Pagination.newBuilder().setSize(5).setFrom(0).build()
        SearchCriteria searchCriteria = SearchCriteria.newBuilder()
                .addSearchCondition(searchCondition)
                .addSortingCriteria(sortingCriteria)
                .setPagination(pagination)
                .build();

        when:
        var response = requestBuilder.buildRequest(indexName, searchCriteria);

        then:
        response.sort().first().field().order() == sortOrder
        response.sort().first().field().field() == sortFieldName

        if (sortFieldName.contains(".")) {
            response.sort().first().field().nested().path() == sortFieldName.split("\\.")[0]
        }

        where:
        indexName           | sortFieldName | sortDirection     | sortOrder
        "root_field_sort"   | "foo"         | SortingOrder.ASC  | SortOrder.Asc
        "nested_field_sort" | "foo.bar"     | SortingOrder.ASC  | SortOrder.Asc
        "root_descending"   | "foo"         | SortingOrder.DESC | SortOrder.Desc
        "nested_descending" | "foo.bar"     | SortingOrder.DESC | SortOrder.Desc
    }

    @Unroll
    def "AddSorts #sortType"() {
        given:
        var requestBuilder = new RequestBuilder()
        var searchCriteria = SearchCriteria.newBuilder().addSortingCriteria(sortingCriteria).build()
        var searchBuilder = new SearchRequest.Builder()

        when:
        requestBuilder.addSorts(searchCriteria, searchBuilder)

        then:
        searchBuilder.build().sort().get(0)._kind() == sortType

        where:
        sortingCriteria                                                                                                 | sortType
        createScriptSort("This-is-my-script-id", ScriptSortType.STRING)                                                 | SortOptions.Kind.Script
        createGeoDistanceSort("field_name", SortingOrder.ASC, DistanceType.ARC, DistanceUnit.MILES, -115.2479, 35.9935) | SortOptions.Kind.GeoDistance
        createScoreSort(SortingOrder.ASC)                                                                               | SortOptions.Kind.Score
        createFieldSort("field_name", SortingOrder.ASC)                                                                 | SortOptions.Kind.Field
    }

    def "buildLayerQuery"() {
        given:
        RequestBuilder requestBuilder = new RequestBuilder()

        FlatStructProtos.FlatValue flatValue = FlatStructProtos.FlatValue.newBuilder()
                .setStringValue("John")
                .build()
        FieldCondition fieldCondition = FieldCondition.newBuilder()
                .setOperation(SearchOperationType.EQUALS)
                .setField("name")
                .setValue(flatValue)
                .build()
        SearchCondition searchCondition = SearchCondition.newBuilder()
                .addFieldCondition(fieldCondition)
                .build()
        SortingCriteria sortingCriteria = SortingCriteria.newBuilder().setFieldSort(
                FieldSort.newBuilder().setFieldName("foo").setOrder(SortingOrder.ASC).build()).build()
        Pagination pagination = Pagination.newBuilder().setSize(5).setFrom(0).build()
        SearchCriteria searchCriteria = SearchCriteria.newBuilder()
                .addSearchCondition(searchCondition)
                .addSortingCriteria(sortingCriteria)
                .setPagination(pagination)
                .build();

        when:
        var query = requestBuilder.buildLayerQuery(indexNames, searchCriteria);

        then:
        query instanceof Query
        query.toString().contains("John")
        query.toString().contains(indexNames[0])

        where:
        indexNames                | nameCase
        ["more_than", "one_name"] | "multiple index names"
        ["single_name"]           | "single index name"
    }

    def "boolQueryBuilder"() {
        given:
        RequestBuilder requestBuilder = new RequestBuilder()

        FlatStructProtos.FlatValue flatValue = FlatStructProtos.FlatValue.newBuilder()
                .setStringValue("John")
                .build()
        FieldCondition fieldCondition = FieldCondition.newBuilder()
                .setOperation(SearchOperationType.EQUALS)
                .setField("name")
                .setValue(flatValue)
                .build()
        SearchCondition searchCondition = SearchCondition.newBuilder()
                .addFieldCondition(fieldCondition)
                .build()
        Pagination pagination = Pagination.newBuilder().setSize(5).setFrom(0).build()
        SearchCriteria searchCriteria = SearchCriteria.newBuilder()
                .addSearchCondition(searchCondition)
                .setPagination(pagination)
                .build();

        when:
        var resp = requestBuilder.boolQueryBuilder(searchCriteria, BoolType.MUST)

        then:
        resp instanceof BoolQuery.Builder
    }


    SortingCriteria "createScriptSort"(String script, ScriptSortType scriptType) {
        SortingCriteria.newBuilder().setScriptSort(ScriptSort.newBuilder().setScriptOrId(script)
                .setSortType(scriptType)).build()
    }

    SortingCriteria "createGeoDistanceSort"(String fieldName, SortingOrder sortOrder, DistanceType distanceType,
                                            DistanceUnit distanceUnit, double lon, double lat) {
        SortingCriteria.newBuilder().setGeoDistanceSort(GeoDistanceSort.newBuilder().setFieldName(fieldName)
                .setOrder(sortOrder).setDistanceType(distanceType).setDistanceUnit(distanceUnit)
                .setPoint(FlatStructProtos.LatLon.newBuilder().setLon(lon).setLat(lat))).build()
    }

    SortingCriteria "createScoreSort"(SortingOrder sortingOrder) {
        SortingCriteria.newBuilder().setScoreSort(ScoreSort.newBuilder().setOrder(sortingOrder)).build()
    }

    SortingCriteria "createFieldSort"(String fieldName, SortingOrder sortingOrder) {
        SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder().setOrder(sortingOrder).setFieldName(fieldName)).build()
    }
}
