package com.phatjam98.elasticprotos.micronaut;

import com.phatjam98.elasticprotos.query.AggregationResults;
import java.util.List;

/**
 * The outcome of {@link ElasticProtosService#aggregate}: aggregation results without hits.
 *
 * @param results the results of the criteria's aggregations
 * @param total the number of matching documents, exact or a lower bound; {@code -1} ({@link
 *     com.phatjam98.elasticprotos.engine.SearchResult#NO_TOTAL}) when the total is not tracked
 * @param warnings non-fatal problems reported by the query compiler
 */
public record AggregationPage(AggregationResults results, long total, List<String> warnings) {

  /** Makes the warnings unmodifiable; null results become empty. */
  public AggregationPage {
    results = results == null ? AggregationResults.empty() : results;
    warnings = List.copyOf(warnings);
  }
}
