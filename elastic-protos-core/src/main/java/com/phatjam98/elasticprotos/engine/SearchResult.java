package com.phatjam98.elasticprotos.engine;

import java.util.List;
import java.util.Map;

/**
 * The outcome of a search.
 *
 * @param total the total hit count, exact or a lower bound
 * @param totalExact whether {@code total} is exact
 * @param hits the hits in order
 * @param pitId the point-in-time id to use for the next page, when the search used one
 * @param aggregations raw aggregation results, possibly empty
 * @param tookMs engine-side time
 */
public record SearchResult(
    long total,
    boolean totalExact,
    List<Hit> hits,
    String pitId,
    Map<String, Object> aggregations,
    long tookMs) {

  /** Returns the sort values of the last hit, for {@code search_after}, or null when empty. */
  public List<Object> lastSort() {
    return hits.isEmpty() ? null : hits.get(hits.size() - 1).sort();
  }
}
