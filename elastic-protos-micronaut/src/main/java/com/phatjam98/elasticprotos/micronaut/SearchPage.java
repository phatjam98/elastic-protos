package com.phatjam98.elasticprotos.micronaut;

import com.google.protobuf.Message;
import com.phatjam98.elasticprotos.engine.Hit;
import com.phatjam98.elasticprotos.proto.Pagination;
import java.util.List;

/**
 * One page of search results read back into messages.
 *
 * @param items the hits as messages, in hit order
 * @param hits the raw hits, with index, id, score and sort values
 * @param total the total hit count, exact or a lower bound
 * @param totalExact whether {@code total} is exact
 * @param next the pagination that requests the following page; set it on the criteria
 * @param warnings non-fatal problems reported by the query compiler
 * @param <T> the message type
 */
public record SearchPage<T extends Message>(
    List<T> items,
    List<Hit> hits,
    long total,
    boolean totalExact,
    Pagination next,
    List<String> warnings) {

  /** Makes the lists unmodifiable. */
  public SearchPage {
    items = List.copyOf(items);
    hits = List.copyOf(hits);
    warnings = List.copyOf(warnings);
  }
}
