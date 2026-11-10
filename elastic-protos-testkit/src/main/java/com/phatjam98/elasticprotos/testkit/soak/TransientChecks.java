package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.testkit.soak.LogComparison.Anomaly;
import com.phatjam98.elasticprotos.testkit.soak.LogComparison.Timeline;
import com.phatjam98.elasticprotos.testkit.soak.OpLog.Entry;
import com.phatjam98.elasticprotos.testkit.soak.WriteFailures.Mode;
import java.util.List;
import java.util.Map;

/**
 * Classifies the transient evidence of a run, which the final comparison cannot see when a later
 * full write healed the document: stale reads, and writes whose result contradicted the log
 * ({@link Writers.Observation}).
 *
 * <p>Zero downtime: before the alias can have moved, nothing is documented. Until the reconcile
 * bound, reads may lack writes made after the catch-up and writes may find the new index stale
 * (README "Zero-downtime reindex"): {@code RECONCILE_WINDOW}. Until the first {@code SWEPT}, a
 * document deleted from the old index during the copy can still be read or found ({@code
 * GHOST_BEFORE_SWEEP}, not in the README yet). After that, only a document left in a documented
 * anomaly explains it. Blocking reindex and backfill document nothing of the kind.
 */
public final class TransientChecks {

  /** The class of an unexplained item. */
  public static final String VIOLATION = "VIOLATION";

  private TransientChecks() {}

  /**
   * Classifies a stale read.
   *
   * @param mode the migration mode
   * @param timeline the migration timeline
   * @param swapNotBefore before this the alias pointed at the old index for sure, or -1 when it
   *     never moved
   * @param started when the read was sent
   * @param at when it returned
   * @param history the acknowledged writes of the id when it was read
   * @param actual what the read returned, or null
   * @return the documented class, or {@link #VIOLATION}
   */
  public static String read(Mode mode, Timeline timeline, long swapNotBefore, long started,
      long at, List<Entry> history, Map<String, Object> actual) {
    return read(null, mode, timeline, swapNotBefore, started, at, history, actual);
  }

  /**
   * Classifies a stale read of a known id, which lets a documented replay residual of a resumed
   * reconcile explain it ({@link LogComparison.Anomaly#RESURRECTED_BY_RESUME_REPLAY}).
   *
   * @param id the document id
   * @param mode the migration mode
   * @param timeline the migration timeline
   * @param swapNotBefore as for the other overload
   * @param started when the read was sent
   * @param at when it returned
   * @param history the acknowledged writes of the id when it was read
   * @param actual what the read returned, or null
   * @return the documented class, or {@link #VIOLATION}
   */
  public static String read(String id, Mode mode, Timeline timeline, long swapNotBefore,
      long started, long at, List<Entry> history, Map<String, Object> actual) {
    if (mode != Mode.ZERO_DOWNTIME || swapNotBefore < 0 || at < swapNotBefore) {
      return VIOLATION;
    }
    if (actual != null && OpLog.replay(history) == null && started < timeline.sweptAt()
        && LogComparison.ghost(history, actual, timeline)) {
      return "GHOST_BEFORE_SWEEP";
    }
    if (started < timeline.reconcileBound()) {
      return "RECONCILE_WINDOW";
    }
    Anomaly anomaly = LogComparison.explain(id, history, actual, timeline);
    return anomaly == null ? VIOLATION : anomaly.name();
  }

  /**
   * Classifies an observation.
   *
   * @param mode the migration mode
   * @param timeline the migration timeline
   * @param swapNotBefore as for {@link #read}
   * @param observation the observation
   * @return the documented class, or {@link #VIOLATION}
   */
  public static String observation(Mode mode, Timeline timeline, long swapNotBefore,
      Writers.Observation observation) {
    if (mode != Mode.ZERO_DOWNTIME || swapNotBefore < 0 || observation.at() < swapNotBefore) {
      return VIOLATION;
    }
    if (observation.startedAt() < timeline.reconcileBound()) {
      return "RECONCILE_WINDOW";
    }
    if (!"createFoundExisting".equals(observation.kind())) {
      return VIOLATION;
    }
    if (observation.startedAt() < timeline.sweptAt()
        && LogComparison.ghostPossible(observation.before(), timeline)) {
      return "GHOST_BEFORE_SWEEP";
    }
    Anomaly shape = LogComparison.resurrectionShape(observation.id(), observation.before(),
        timeline);
    return shape == null ? VIOLATION : shape.name();
  }
}
