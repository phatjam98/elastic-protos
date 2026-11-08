package com.phatjam98.elasticprotos.query;

import java.util.List;
import java.util.Map;

/**
 * A search body ready for {@link com.phatjam98.elasticprotos.engine.SearchEngine#search}.
 *
 * @param body the search body: {@code query}, {@code sort}, paging and {@code track_total_hits}
 * @param warnings non-fatal problems, such as unknown fields skipped in lenient mode
 * @param usesPointInTime whether the body carries a {@code pit}; the engine then ignores indices
 */
public record CompiledSearch(Map<String, Object> body, List<String> warnings,
    boolean usesPointInTime) {

  /** Makes the warnings unmodifiable. */
  public CompiledSearch {
    warnings = List.copyOf(warnings);
  }
}
