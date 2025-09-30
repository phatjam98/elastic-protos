package com.phatjam98.elasticsearch.utils;

import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.phatjam98.protos.service.protos.Pagination;
import com.phatjam98.protos.service.protos.SearchCriteria;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to build Protos from Elasticsearch SearchResponses.
 */
public class ResponseUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResponseUtils.class);

  private ResponseUtils() {
  }

  /**
   * Takes {@link SearchCriteria} and {@link SearchResponse} to construct the new {@link Pagination}
   * for the SearchResponse.
   *
   * @param request ServiceRequest
   * @param response SearchResponse
   * @return Pagination
   */
  public static Pagination getPagination(SearchCriteria request,
                                         SearchResponse<Map<String, Object>> response) {
    var paginationBuilder = Pagination.newBuilder();

    var from = request.getPagination().getFrom();

    if (!response.hits().hits().isEmpty()) {
      var totalHits = response.hits().total();
      var size = response.hits().hits().size();
      int totalHitsCount = totalHits == null ? 0 : (int) totalHits.value();
      paginationBuilder.setTotal(totalHitsCount).setSize(size);
    } else {
      paginationBuilder.setTotal(0).setSize(0);
    }

    var pit = response.pitId();

    if (pit == null) {
      paginationBuilder.setFrom(from);
    } else {
      paginationBuilder.setCursor(pit);
    }

    return paginationBuilder.build();
  }

  /**
   * This is a generic helper to convert a {@link SearchHit} to the provided Protobuf Builder.  This
   * will work with any class that extends {@link GeneratedMessageV3.Builder}.  The reason we
   * return a Builder rather than the Message is we can still add and manipulate the builder before
   * we need to finalize the Message.
   *
   * @param hit SearchHit
   * @param builder GeneratedMessageV3.Builder
   * @param <T> Type of Protobuf Builder to build from the Hit
   * @return GeneratedMessageV3.Builder
   */
  public static <T extends GeneratedMessageV3.Builder> T getBuilderFromHit(@NonNull Hit<?> hit,
                                                                           T builder) {
    if (hit.source() == null) {
      return builder;
    }

    try {
      var mapper = new ObjectMapper();
      var str = mapper.writeValueAsString(hit.source());
      ProtoJsonUtils.convertJsonToProto(str, builder);
    } catch (JsonProcessingException e) {
      LOGGER.error("Error parsing JSON: {}", e.getMessage());
    }

    return builder;
  }
}