package com.phatjam98.elasticprotos.query;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A search body ready for {@link com.phatjam98.elasticprotos.engine.SearchEngine#search}.
 *
 * <p>The body is copied on construction and returned unmodifiable, in its iteration order. The
 * copy is shallow: to add clauses, copy the body into a new map first.
 *
 * @param body the search body: {@code query}, {@code sort}, paging and {@code track_total_hits}
 * @param warnings non-fatal problems, such as unknown fields skipped in lenient mode
 * @param usesPointInTime whether the body carries a {@code pit}; the engine then ignores indices
 */
public record CompiledSearch(Map<String, Object> body, List<String> warnings,
    boolean usesPointInTime) {

  /** Copies the body and the warnings. */
  public CompiledSearch {
    // the same shallow, null-tolerant copy as com.phatjam98.elasticprotos.engine.Copies
    body = Collections.unmodifiableMap(new LinkedHashMap<>(body));
    warnings = List.copyOf(warnings);
  }
}
