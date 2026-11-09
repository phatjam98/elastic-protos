package com.phatjam98.elasticprotos.engine;

import java.util.List;

/**
 * The outcome of a bulk request.
 *
 * <p>{@code items} is copied on construction (a shallow copy) and returned unmodifiable.
 *
 * @param errors whether any item failed
 * @param items one entry per operation, in request order
 * @param tookMs wall time
 */
public record BulkResult(boolean errors, List<Item> items, long tookMs) {

  /** Copies the items. */
  public BulkResult {
    items = items == null ? List.of() : Copies.list(items);
  }

  /**
   * The outcome of one bulk operation.
   *
   * @param op the operation name as the engine reports it
   * @param index the concrete index
   * @param id the document id
   * @param status the HTTP-style status
   * @param error the error reason, or null on success
   */
  public record Item(String op, String index, String id, int status, String error) {

    /** Returns whether the operation succeeded. */
    public boolean ok() {
      return error == null;
    }
  }

  /** Returns the failed items. */
  public List<Item> failures() {
    return items.stream().filter(item -> !item.ok()).toList();
  }
}
