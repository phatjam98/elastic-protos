package com.phatjam98.elasticprotos.engine;

import java.util.List;
import java.util.Map;

/**
 * One search hit, or a document read by id.
 *
 * <p>{@code source} and {@code sort} are copied on construction and returned unmodifiable. The
 * copy is shallow: objects and arrays nested in the document are shared with the caller. Both may
 * hold null values, and either may itself be null. {@code highlight} is copied together with each
 * fragment list, returned unmodifiable, and never null.
 *
 * <p>A search reports {@code version} when its body asks for {@code "version": true}, and {@code
 * seqNo} and {@code primaryTerm} when it asks for {@code "seq_no_primary_term": true}; a read by id
 * reports all three. They are null otherwise.
 *
 * @param index the concrete index the document lives in
 * @param id the document id
 * @param score the relevance score, or null when the search did not score
 * @param source the document body
 * @param sort the sort values, to be passed as {@code search_after} for the next page
 * @param version the document version, or null
 * @param seqNo the sequence number of the last write to the document, or null
 * @param primaryTerm the primary term of the last write to the document, or null
 * @param highlight highlighted fragments per field; empty when the search asked for none
 */
public record Hit(String index, String id, Double score, Map<String, Object> source,
    List<Object> sort, Long version, Long seqNo, Long primaryTerm,
    Map<String, List<String>> highlight) {

  /** Copies the document, the sort values and the highlights. */
  public Hit {
    source = Copies.map(source);
    sort = Copies.list(sort);
    highlight = highlight == null ? Map.of() : Copies.stringLists(highlight);
  }

  /**
   * A hit without version metadata or highlights.
   *
   * @param index the concrete index the document lives in
   * @param id the document id
   * @param score the relevance score, or null
   * @param source the document body
   * @param sort the sort values
   */
  public Hit(String index, String id, Double score, Map<String, Object> source,
      List<Object> sort) {
    this(index, id, score, source, sort, null, null, null, Map.of());
  }
}
