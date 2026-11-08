package com.phatjam98.elasticprotos.engine;

import java.util.List;
import java.util.Map;

/**
 * One search hit.
 *
 * @param index the concrete index the document lives in
 * @param id the document id
 * @param score the relevance score, or null when the search did not score
 * @param source the document body
 * @param sort the sort values, to be passed as {@code search_after} for the next page
 */
public record Hit(String index, String id, Double score, Map<String, Object> source,
    List<Object> sort) {}
