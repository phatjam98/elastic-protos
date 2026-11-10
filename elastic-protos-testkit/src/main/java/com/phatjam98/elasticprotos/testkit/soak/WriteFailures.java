package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.testkit.soak.LogComparison.Timeline;
import com.phatjam98.elasticprotos.testkit.soak.OpLog.Entry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Write failures seen by the soak writers, and their classification against the documented
 * guarantees of the migration mode.
 */
public final class WriteFailures {

  /** How a run migrates, which decides what failures are documented. */
  public enum Mode {
    ZERO_DOWNTIME,
    BLOCKING,
    BACKFILL,
    NONE
  }

  /**
   * One failed write attempt.
   *
   * @param id the document id
   * @param op the operation ({@code CREATE}, {@code INDEX}, {@code UPDATE}, {@code DELETE})
   * @param kind the engine error kind, or the exception class
   * @param status the HTTP status, or 0
   * @param message the error message, shortened
   * @param startedAt when the attempt was sent
   * @param at when it failed
   * @param phase the phase label when it failed
   * @param retried whether the writer retried it
   */
  public record Failure(String id, String op, String kind, int status, String message,
      long startedAt, long at, String phase, boolean retried) {}

  private final List<Failure> failures = new ArrayList<>();

  /** Records a failure. */
  public synchronized void add(Failure failure) {
    failures.add(failure);
  }

  /** Returns the failures so far. */
  public synchronized List<Failure> all() {
    return List.copyOf(failures);
  }

  /**
   * Returns the documented class of a failure, or null when it is a genuine violation.
   *
   * <ul>
   *   <li>{@code BLOCKED_AFTER_SWAP} (zero downtime): a straggler that resolved the alias to the
   *       old index before the swap (sent before it) and was blocked after it, whose retry against
   *       the alias then succeeded (README "Limitations");
   *   <li>{@code UPDATE_404_BEFORE_RECONCILE} (zero downtime): a partial update sent after the
   *       swap and before the reconcile bound, of a document the old index acknowledged after the
   *       catch-up began, which the new index lacks until the reconcile creates it (README
   *       "Zero-downtime reindex");
   *   <li>{@code BLOCKING_WRITE_BLOCK} (blocking reindex): a write sent before the swap, during
   *       the write block.
   * </ul>
   *
   * @param failure the failure
   * @param mode the migration mode
   * @param timeline the migration timeline
   * @param history the acknowledged writes of the failure's id
   * @return the documented class, or null
   */
  public static String documented(Failure failure, Mode mode, Timeline timeline,
      List<Entry> history) {
    boolean swapped = timeline.swappedAt() > 0 && failure.at() >= timeline.swappedAt();
    switch (mode) {
      case ZERO_DOWNTIME:
        if ("BLOCKED".equals(failure.kind()) && swapped
            && failure.startedAt() < timeline.swappedAt() && retriedOk(failure, history)) {
          return "BLOCKED_AFTER_SWAP";
        }
        if ("NOT_FOUND".equals(failure.kind()) && "UPDATE".equals(failure.op()) && swapped
            && failure.startedAt() < timeline.reconcileBound()
            && createdLateOnOld(failure, timeline, history)) {
          return "UPDATE_404_BEFORE_RECONCILE";
        }
        return null;
      case BLOCKING:
        return "BLOCKED".equals(failure.kind()) && timeline.swappedAt() > 0
            && failure.startedAt() <= timeline.swappedAt() ? "BLOCKING_WRITE_BLOCK" : null;
      default:
        return null;
    }
  }

  /** Whether a retried write was later acknowledged, or the writer stopped instead of retrying. */
  private static boolean retriedOk(Failure failure, List<Entry> history) {
    if (!failure.retried()) {
      return true;
    }
    for (Entry entry : history) {
      if (entry.startedAt() >= failure.at()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether the document a failed write addressed was last created by a write the old index
   * acknowledged after the catch-up began, and only written there since.
   */
  private static boolean createdLateOnOld(Failure failure, Timeline timeline,
      List<Entry> history) {
    List<Entry> before = new ArrayList<>();
    for (Entry entry : history) {
      if (entry.ackedAt() <= failure.startedAt()) {
        before.add(entry);
      }
    }
    Map<String, Object> state = null;
    int creation = -1;
    for (int i = 0; i < before.size(); i++) {
      Map<String, Object> next = OpLog.replay(state, List.of(before.get(i)));
      if (state == null && next != null) {
        creation = i;
      }
      state = next;
    }
    if (creation < 0 || state == null) {
      return false;
    }
    for (int i = creation; i < before.size(); i++) {
      Entry entry = before.get(i);
      if (!timeline.oldIndex().equals(entry.index()) || entry.startedAt() < timeline.copiedAt()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Summarises the failures: counts by phase and kind, by documented class, the violations, and
   * samples.
   *
   * @param mode the migration mode
   * @param timeline the migration timeline
   * @param log the acknowledged writes by id
   * @param sampleLimit how many violations to list
   * @return the summary as JSON-shaped maps; {@code violations} holds the count of genuine ones
   */
  public synchronized Map<String, Object> summarize(Mode mode, Timeline timeline,
      Map<String, List<Entry>> log, int sampleLimit) {
    Map<String, Long> byPhaseKind = new TreeMap<>();
    Map<String, Long> byClass = new TreeMap<>();
    List<Object> samples = new ArrayList<>();
    long violations = 0;
    for (Failure failure : failures) {
      byPhaseKind.merge(failure.phase() + "/" + failure.op() + "/" + failure.kind(), 1L,
          Long::sum);
      String documented = documented(failure, mode, timeline,
          log.getOrDefault(failure.id(), List.of()));
      byClass.merge(documented == null ? "VIOLATION" : documented, 1L, Long::sum);
      if (documented == null) {
        violations++;
        if (samples.size() < sampleLimit) {
          Map<String, Object> sample = new LinkedHashMap<>();
          sample.put("id", failure.id());
          sample.put("op", failure.op());
          sample.put("kind", failure.kind());
          sample.put("status", failure.status());
          sample.put("phase", failure.phase());
          sample.put("message", failure.message());
          sample.put("atSwapMs", timeline.swappedAt() > 0
              ? failure.at() - timeline.swappedAt() : null);
          samples.add(sample);
        }
      }
    }
    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("attemptsFailed", (long) failures.size());
    summary.put("byPhaseOpKind", byPhaseKind);
    summary.put("byClass", byClass);
    summary.put("violations", violations);
    summary.put("violationSamples", samples);
    return summary;
  }
}
