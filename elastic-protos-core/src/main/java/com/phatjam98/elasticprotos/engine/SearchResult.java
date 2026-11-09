package com.phatjam98.elasticprotos.engine;

import java.util.List;
import java.util.Map;

/**
 * The outcome of a search.
 *
 * <p>A search sent with {@code track_total_hits: false} gets no hit count back; {@code total} is
 * then {@link #NO_TOTAL} ({@code -1}) and {@code totalExact} false. Check {@link #hasTotal()}
 * before using {@code total}.
 *
 * @param total the total hit count, exact or a lower bound, or {@link #NO_TOTAL} when the engine
 *     did not count hits
 * @param totalExact whether {@code total} is exact; false when there is no total
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

  /** The {@link #total()} of a search that did not count hits. */
  public static final long NO_TOTAL = -1L;

  /** Returns whether the engine counted hits, so that {@link #total()} is meaningful. */
  public boolean hasTotal() {
    return total != NO_TOTAL;
  }

  /** Returns the sort values of the last hit, for {@code search_after}, or null when empty. */
  public List<Object> lastSort() {
    return hits.isEmpty() ? null : hits.get(hits.size() - 1).sort();
  }
}
