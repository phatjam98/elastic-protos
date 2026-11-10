package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.engine.Hit;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Decides whether a document a resumed reconcile created a second time lies in the documented
 * replay residual (ADR 0007, second addendum): the reconcile records its progress in the marker
 * ({@code pending.progress}: completed passes in {@code done}, per pass a cursor of the last
 * sequence number processed and the ids at it), at most every four pages that wrote and five
 * seconds; a killed run's pages after its last record are replayed, and a document of those pages
 * that it created and the application deleted is created again.
 *
 * <p>A document created in the old index after the catch-up is read by the hot pass, in {@code
 * _seq_no} order. It is in the residual only when all of these hold, taken from the marker as the
 * killed run left it:
 *
 * <ul>
 *   <li>the library records progress (its {@code ReconcileStats} carry a {@code strategy}); a
 *       library that does not replays the whole reconcile, which is the defect, not a residual;
 *   <li>the hot pass is not in {@code done};
 *   <li>the document's {@code _seq_no} in the frozen old index sorts after the hot cursor (above
 *       its sequence number, or at it and not among its ids), or there is no hot cursor yet;
 *   <li>at most {@link #MAX_REPLAYED} documents of the old index lie between the cursor (or the
 *       checkpoint when there is none) and the document, the four pages the library may leave
 *       unrecorded.
 * </ul>
 */
final class ReplayResidual implements Predicate<String> {

  /** Four pages of 1,000 documents: what the library may process without recording. */
  static final long MAX_REPLAYED = 4000;

  private final SearchEngine engine;
  private final String oldIndex;
  private final Map<String, Object> marker;
  private final boolean recordsProgress;
  private final List<Object> evidence;

  /**
   * Creates the predicate.
   *
   * @param engine the engine, to read the frozen old index
   * @param oldIndex the old index
   * @param marker {@code _meta.elastic_protos.pending} of the new index right after the kill
   * @param recordsProgress whether the library records reconcile progress
   * @param evidence receives one entry per id decided
   */
  ReplayResidual(SearchEngine engine, String oldIndex, Map<String, Object> marker,
      boolean recordsProgress, List<Object> evidence) {
    this.engine = engine;
    this.oldIndex = oldIndex;
    this.marker = marker;
    this.recordsProgress = recordsProgress;
    this.evidence = evidence;
  }

  @Override
  public boolean test(String id) {
    Long seqNo = null;
    try {
      Optional<Hit> hit = engine.get(oldIndex, id);
      seqNo = hit.map(Hit::seqNo).orElse(null);
    } catch (RuntimeException e) {
      // undecidable: not a residual
    }
    final Long at = seqNo;
    Map<String, Object> decision = decide(marker, recordsProgress, id, seqNo,
        base -> countBetween(base, at));
    synchronized (evidence) {
      evidence.add(decision);
    }
    return Boolean.TRUE.equals(decision.get("residual"));
  }

  private long countBetween(long base, Long seqNo) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("size", 0);
    body.put("track_total_hits", true);
    body.put("query", Map.of("range", Map.of("_seq_no", Map.of("gt", base, "lte", seqNo))));
    return engine.search(List.of(oldIndex), body).total();
  }

  /**
   * Decides, without the engine, whether a document is in the replay residual.
   *
   * @param marker the pending marker at the kill
   * @param recordsProgress whether the library records reconcile progress
   * @param id the document id
   * @param seqNo the document's sequence number in the frozen old index, or null when unknown
   * @param ranker counts the old index's documents above a base sequence number up to the
   *     document's
   * @return the decision with its evidence; {@code residual} is the verdict
   */
  static Map<String, Object> decide(Map<String, Object> marker, boolean recordsProgress,
      String id, Long seqNo, java.util.function.LongUnaryOperator ranker) {
    Map<String, Object> decision = new LinkedHashMap<>();
    decision.put("id", id);
    decision.put("seqNo", seqNo);
    Map<String, Object> progress = Cluster.map(marker.get("progress"));
    Map<String, Object> cursor = Cluster.map(Cluster.map(progress.get("cursors")).get("hot"));
    Object done = progress.get("done");
    decision.put("done", done);
    decision.put("hotCursor", cursor.isEmpty() ? null : cursor);
    decision.put("checkpoint", marker.get("checkpoint"));
    String reason = null;
    if (!recordsProgress) {
      reason = "the library records no reconcile progress: a full replay is not a residual";
    } else if (marker.isEmpty()) {
      reason = "no marker at the kill";
    } else if (done instanceof List<?> passes && passes.contains("hot")) {
      reason = "the hot pass was recorded done before the kill";
    } else if (seqNo == null) {
      reason = "not found in the old index";
    }
    long base;
    if (reason == null && !cursor.isEmpty()) {
      long at = ((Number) cursor.get("seq_no")).longValue();
      Object ids = cursor.get("ids");
      boolean processed = seqNo < at
          || seqNo == at && ids instanceof List<?> list && list.contains(id);
      if (processed) {
        reason = "at or before the recorded hot cursor";
      }
      base = at;
    } else {
      Object checkpoint = marker.get("checkpoint");
      base = checkpoint instanceof Number number ? number.longValue() : -1;
    }
    if (reason == null) {
      long rank = ranker.applyAsLong(base);
      decision.put("documentsAfterCursor", rank);
      if (rank > MAX_REPLAYED) {
        reason = rank + " documents after the recorded position, more than the " + MAX_REPLAYED
            + " the library may leave unrecorded";
      }
    }
    decision.put("residual", reason == null);
    decision.put("reason", reason == null ? "after the last progress record, within "
        + MAX_REPLAYED + " documents" : reason);
    return decision;
  }
}
