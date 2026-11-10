package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.testkit.soak.OpLog.Entry;
import com.phatjam98.elasticprotos.testkit.soak.OpLog.OpType;
import com.phatjam98.elasticprotos.util.Json;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Compares the documents behind an alias with the operation log and sorts every difference into a
 * documented anomaly of the zero-downtime reindex or a genuine violation.
 *
 * <p>Documented anomalies (README "Zero-downtime reindex", ADR 0007), recognised only when the
 * timeline is a zero-downtime reindex, and only when the write that caused them was sent before
 * the reconcile could have reached the document ({@link Timeline#reconcileBound()}): the README
 * documents each of them "before the reconcile reached it". The same symptom produced later (for
 * example by a resumed reconcile) is a violation.
 *
 * <ul>
 *   <li>{@link Anomaly#STALE_PARTIAL_UPDATE}: the first write of the id the new index acknowledged
 *       is a partial update sent before the reconcile bound, and the document equals the replay of
 *       the writes after it onto an earlier state of the log, every write skipped in between having
 *       been acknowledged by the old index and sent after the catch-up pass started. The update
 *       applied to the stale copy and the reconcile kept it (409).
 *   <li>{@link Anomaly#STALE_PARTIAL_UPDATE_ON_GHOST}: the same, where the stale copy is the one
 *       the first copy took (the base write acknowledged before the first {@code PREPARED}),
 *       because the document was deleted from the old index before the catch-up read it (deletes
 *       reach the new index only at the sweep). Not in the README yet; named apart.
 *   <li>{@link Anomaly#RESURRECTED_AFTER_DELETE}: the old index last created the document after
 *       the catch-up began, every write of the new index was sent before the reconcile bound and
 *       the first of them found nothing there, the last is a delete, and the alias holds the old
 *       index's final version: the reconcile created it again.
 * </ul>
 *
 * <p>Everything else that differs is a violation: missing (the log has it, the alias does not),
 * extra (the alias has it, the log does not) or differing.
 */
public final class LogComparison {

  /** Documented anomalies of the zero-downtime reindex. */
  public enum Anomaly {
    STALE_PARTIAL_UPDATE,
    /**
     * {@link #STALE_PARTIAL_UPDATE} where the stale copy predates the catch-up: the document was
     * deleted from the old index during the copy (the catch-up copies no deletes, the sweep
     * removes them), so the new index still held its copy-time version at the swap.
     */
    STALE_PARTIAL_UPDATE_ON_GHOST,
    RESURRECTED_AFTER_DELETE,
    /**
     * The documented residual of a resumed reconcile (ADR 0007, second addendum): the shape of
     * {@link #RESURRECTED_AFTER_DELETE}, except that an earlier, killed reconcile had already
     * created the document once, and the resume created it again because its position lay in the
     * pages processed after the last progress record. Only recognised when {@link
     * Timeline#replayResidual()} accepts the id, which the kill scenario decides from the marker.
     */
    RESURRECTED_BY_RESUME_REPLAY
  }

  /** How a document differs when it is not explained by a documented anomaly. */
  public enum Violation {
    MISSING,
    EXTRA,
    DIFFERING
  }

  /**
   * What the comparison needs to know about the migration.
   *
   * @param oldIndex the index the alias pointed at before the migration
   * @param preparedAt when the last {@code PREPARED} phase before the effective swap was
   *     reported, epoch millis: the first copy starts right after it
   * @param copiedAt when the last {@code COPIED} phase before the effective swap was reported,
   *     epoch millis: writes sent after it may have missed the catch-up pass
   * @param caughtUpAt when the last {@code CAUGHT_UP} before the effective swap was reported: a
   *     version acknowledged after it was read by no copy pass
   * @param snapshotAt when the last {@code SNAPSHOT} before the effective swap was reported: the
   *     alias swap request is sent after it, so before it the alias surely pointed at the old index
   * @param swappedAt when the effective {@code SWAPPED} phase was reported, epoch millis, or 0
   * @param reconcileBound the first {@code RECONCILED}: after it the reconcile has reached every
   *     document (when a run died while reconciling, this is the resumed run's); {@link
   *     Long#MAX_VALUE} when no reconcile completed
   * @param sweptAt the first {@code SWEPT}: ghosts (documents deleted from the old index during
   *     the copy) are gone from the new index after it; {@link Long#MAX_VALUE} when none
   * @param zeroDowntime whether the documented anomalies of the zero-downtime reindex apply
   * @param replayResidual accepts the ids that a resumed reconcile may create a second time
   *     (pages the killed run processed after its last progress record); none by default
   */
  public record Timeline(String oldIndex, long preparedAt, long copiedAt, long caughtUpAt,
      long snapshotAt, long swappedAt, long reconcileBound, long sweptAt, boolean zeroDowntime,
      java.util.function.Predicate<String> replayResidual) {

    /** A timeline that accepts no replay residual. */
    public Timeline(String oldIndex, long preparedAt, long copiedAt, long caughtUpAt,
        long snapshotAt, long swappedAt, long reconcileBound, long sweptAt,
        boolean zeroDowntime) {
      this(oldIndex, preparedAt, copiedAt, caughtUpAt, snapshotAt, swappedAt, reconcileBound,
          sweptAt, zeroDowntime, id -> false);
    }

    /** Returns a copy accepting these ids as the replay residual of a resumed reconcile. */
    public Timeline withReplayResidual(java.util.function.Predicate<String> ids) {
      return new Timeline(oldIndex, preparedAt, copiedAt, caughtUpAt, snapshotAt, swappedAt,
          reconcileBound, sweptAt, zeroDowntime, ids);
    }

    /** A timeline without documented anomalies (blocking reindex, backfill, no migration). */
    public static Timeline strict(String oldIndex) {
      return new Timeline(oldIndex, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, 0, 0,
          Long.MAX_VALUE,
          Long.MAX_VALUE, false);
    }

    /** A strict timeline that knows when the alias moved (blocking reindex). */
    public static Timeline strict(String oldIndex, long swappedAt) {
      return new Timeline(oldIndex, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, swappedAt,
          swappedAt,
          Long.MAX_VALUE,
          Long.MAX_VALUE, false);
    }
  }

  /**
   * One difference, with what the log and the alias hold.
   *
   * @param id the document id
   * @param kind the violation or anomaly name
   * @param expected the document the log leaves behind, or null
   * @param actual the document behind the alias, or null
   * @param history the writes of the id, abbreviated
   */
  public record Finding(String id, String kind, Map<String, Object> expected,
      Map<String, Object> actual, List<String> history) {}

  /**
   * The outcome.
   *
   * @param checked ids compared
   * @param matched ids whose document is what the log expects
   * @param uncertain ids left out because a write had an unknown outcome
   * @param violations genuine violations by kind
   * @param anomalies documented anomalies by kind
   * @param findings the differences, violations first, at most {@code sampleLimit} of each kind
   * @param differing the ids of every difference, violation or anomaly
   */
  public record Result(long checked, long matched, long uncertain,
      Map<Violation, Long> violations, Map<Anomaly, Long> anomalies, List<Finding> findings,
      Set<String> differing) {

    /** Returns the number of genuine violations. */
    public long violationCount() {
      return violations.values().stream().mapToLong(Long::longValue).sum();
    }

    /** Returns the number of documented anomalies. */
    public long anomalyCount() {
      return anomalies.values().stream().mapToLong(Long::longValue).sum();
    }

    /** Returns the result as JSON-shaped maps. */
    public Map<String, Object> toMap() {
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("checked", checked);
      map.put("matched", matched);
      map.put("uncertainSkipped", uncertain);
      Map<String, Object> v = new LinkedHashMap<>();
      violations.forEach((kind, count) -> v.put(kind.name(), count));
      map.put("violations", v);
      Map<String, Object> a = new LinkedHashMap<>();
      anomalies.forEach((kind, count) -> a.put(kind.name(), count));
      map.put("documentedAnomalies", a);
      List<Object> samples = new ArrayList<>();
      for (Finding finding : findings) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("id", finding.id());
        f.put("kind", finding.kind());
        f.put("expected", finding.expected());
        f.put("actual", finding.actual());
        f.put("history", finding.history());
        samples.add(f);
      }
      map.put("samples", samples);
      return map;
    }
  }

  private LogComparison() {}

  /**
   * Compares.
   *
   * @param log the acknowledged writes by id
   * @param uncertain ids whose state is unknown
   * @param actual the documents behind the alias by id
   * @param timeline the migration
   * @param sampleLimit how many findings of each kind to keep
   * @return the result
   */
  public static Result compare(Map<String, List<Entry>> log, Set<String> uncertain,
      Map<String, Map<String, Object>> actual, Timeline timeline, int sampleLimit) {
    Map<Violation, Long> violations = new TreeMap<>();
    for (Violation kind : Violation.values()) {
      violations.put(kind, 0L);
    }
    Map<Anomaly, Long> anomalies = new TreeMap<>();
    for (Anomaly kind : Anomaly.values()) {
      anomalies.put(kind, 0L);
    }
    Map<String, Integer> sampled = new TreeMap<>();
    List<Finding> violationFindings = new ArrayList<>();
    List<Finding> anomalyFindings = new ArrayList<>();
    Set<String> differing = new TreeSet<>();
    long checked = 0;
    long matched = 0;
    long skipped = 0;
    Set<String> ids = new TreeSet<>(log.keySet());
    ids.addAll(actual.keySet());
    for (String id : ids) {
      if (uncertain.contains(id)) {
        skipped++;
        continue;
      }
      checked++;
      List<Entry> entries = log.getOrDefault(id, List.of());
      Map<String, Object> expected = OpLog.replay(entries);
      Map<String, Object> found = actual.get(id);
      if (same(expected, found)) {
        matched++;
        continue;
      }
      differing.add(id);
      Anomaly anomaly = timeline.zeroDowntime() ? explain(id, entries, found, timeline) : null;
      String kind;
      if (anomaly != null) {
        anomalies.merge(anomaly, 1L, Long::sum);
        kind = anomaly.name();
      } else {
        Violation violation = expected == null ? Violation.EXTRA
            : found == null ? Violation.MISSING : Violation.DIFFERING;
        violations.merge(violation, 1L, Long::sum);
        kind = violation.name();
      }
      int count = sampled.merge(kind, 1, Integer::sum);
      if (count <= sampleLimit) {
        Finding finding = new Finding(id, kind, expected, found, describe(entries, timeline));
        (anomaly == null ? violationFindings : anomalyFindings).add(finding);
      }
    }
    List<Finding> findings = new ArrayList<>(violationFindings);
    findings.addAll(anomalyFindings);
    return new Result(checked, matched, skipped, violations, anomalies, findings, differing);
  }

  /**
   * Returns the documented anomaly that explains why {@code actual} differs from the replay of
   * {@code entries}, or null.
   */
  public static Anomaly explain(List<Entry> entries, Map<String, Object> actual,
      Timeline timeline) {
    return explain(null, entries, actual, timeline);
  }

  /**
   * Returns the documented anomaly that explains why {@code actual} differs from the replay of
   * {@code entries}, or null; with the id, the replay residual of a resumed reconcile is
   * considered too.
   */
  public static Anomaly explain(String id, List<Entry> entries, Map<String, Object> actual,
      Timeline timeline) {
    if (actual == null) {
      return null;
    }
    if (!timeline.zeroDowntime()) {
      return null;
    }
    if (resurrected(entries, actual, timeline)) {
      return Anomaly.RESURRECTED_AFTER_DELETE;
    }
    if (id != null && resurrectedTwice(entries, actual, timeline)
        && timeline.replayResidual().test(id)) {
      return Anomaly.RESURRECTED_BY_RESUME_REPLAY;
    }
    return staleUpdate(entries, actual, timeline);
  }

  /**
   * Whether a read that found {@code actual} where the log holds the document as deleted saw a
   * ghost: a version a copy pass could have read from the old index (acknowledged before the last
   * {@code CAUGHT_UP}), deleted from the old index afterwards, with no write of the new index
   * since. The sweep removes such documents; until then reads return them.
   */
  public static boolean ghost(List<Entry> entries, Map<String, Object> actual,
      Timeline timeline) {
    if (actual == null || OpLog.replay(entries) != null) {
      return false;
    }
    for (Entry entry : entries) {
      if (onNew(entry, timeline)) {
        return false;
      }
    }
    for (int i = 0; i < entries.size(); i++) {
      if (entries.get(i).ackedAt() < timeline.caughtUpAt()
          && same(OpLog.replay(entries.subList(0, i + 1)), actual)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether the log of an id that it holds as deleted could leave a ghost in the new index: only
   * the old index wrote it, and a version existed that a copy pass could have read (acknowledged
   * before the last {@code CAUGHT_UP}).
   */
  public static boolean ghostPossible(List<Entry> entries, Timeline timeline) {
    if (OpLog.replay(entries) != null) {
      return false;
    }
    for (Entry entry : entries) {
      if (onNew(entry, timeline)) {
        return false;
      }
    }
    for (int i = 0; i < entries.size(); i++) {
      if (entries.get(i).ackedAt() < timeline.caughtUpAt()
          && OpLog.replay(entries.subList(0, i + 1)) != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether the log of an id it holds as deleted has the shape of a documented resurrection
   * ({@link Anomaly#RESURRECTED_AFTER_DELETE}): the reconcile created the old index's final
   * version again.
   */
  public static boolean resurrectedShape(List<Entry> entries, Timeline timeline) {
    return resurrectionShape(null, entries, timeline) == Anomaly.RESURRECTED_AFTER_DELETE;
  }

  /**
   * Returns the resurrection class the log of an id it holds as deleted has the shape of: {@link
   * Anomaly#RESURRECTED_AFTER_DELETE}, {@link Anomaly#RESURRECTED_BY_RESUME_REPLAY} (with the id
   * accepted by the timeline's replay residual), or null.
   */
  public static Anomaly resurrectionShape(String id, List<Entry> entries, Timeline timeline) {
    if (!timeline.zeroDowntime()) {
      return null;
    }
    List<Entry> onOld = new ArrayList<>();
    for (Entry entry : entries) {
      if (onNew(entry, timeline)) {
        break;
      }
      onOld.add(entry);
    }
    Map<String, Object> last = OpLog.replay(onOld);
    if (last == null) {
      return null;
    }
    if (resurrected(entries, last, timeline)) {
      return Anomaly.RESURRECTED_AFTER_DELETE;
    }
    return id != null && resurrectedTwice(entries, last, timeline)
        && timeline.replayResidual().test(id) ? Anomaly.RESURRECTED_BY_RESUME_REPLAY : null;
  }

  /**
   * The shape of a second creation: as {@link #resurrected}, except that the new index held the
   * document before the last delete through the alias (its first write there found it, or a later
   * one found it while the log held it deleted), so an earlier reconcile had created it already.
   */
  private static boolean resurrectedTwice(List<Entry> entries, Map<String, Object> actual,
      Timeline timeline) {
    int n = entries.size();
    if (n < 2) {
      return false;
    }
    Entry last = entries.get(n - 1);
    if (last.type() != OpType.DELETE || !onNew(last, timeline)) {
      return false;
    }
    int first = 0;
    while (first < n && !onNew(entries.get(first), timeline)) {
      first++;
    }
    for (int i = first; i < n; i++) {
      if (!onNew(entries.get(i), timeline)
          || entries.get(i).startedAt() >= timeline.reconcileBound()) {
        return false;
      }
    }
    List<Entry> onOld = entries.subList(0, first);
    if (!same(OpLog.replay(onOld), actual) || !createdLateOnOld(onOld, timeline)) {
      return false;
    }
    boolean createdBefore = entries.get(first).found()
        && entries.get(first).type() != OpType.UPDATE;
    Map<String, Object> logState = OpLog.replay(entries.subList(0, first + 1));
    for (int i = first + 1; i < n && !createdBefore; i++) {
      Entry entry = entries.get(i);
      createdBefore = logState == null && entry.found();
      logState = OpLog.replay(logState, List.of(entry));
    }
    return createdBefore;
  }

  /** Whether the old index last created the document after the catch-up began. */
  private static boolean createdLateOnOld(List<Entry> onOld, Timeline timeline) {
    Map<String, Object> state = null;
    int creation = -1;
    for (int i = 0; i < onOld.size(); i++) {
      Map<String, Object> next = OpLog.replay(state, List.of(onOld.get(i)));
      if (state == null && next != null) {
        creation = i;
      }
      state = next;
    }
    if (creation < 0) {
      return false;
    }
    for (int i = creation; i < onOld.size(); i++) {
      if (!lateOnOld(onOld.get(i), timeline)) {
        return false;
      }
    }
    return true;
  }

  /** Whether a write was acknowledged by the old index and sent after the catch-up began. */
  private static boolean lateOnOld(Entry entry, Timeline timeline) {
    return timeline.oldIndex().equals(entry.index()) && entry.startedAt() >= timeline.copiedAt();
  }

  private static boolean onNew(Entry entry, Timeline timeline) {
    return entry.index() != null && !timeline.oldIndex().equals(entry.index());
  }

  /**
   * The reconcile created a document again that was deleted through the alias after the swap:
   * the old index last created it after the catch-up began (so the snapshot never held it), the
   * new index's writes of it end with that delete, and the alias holds the old index's final
   * version. The first write of the new index found nothing there (a delete of a missing document,
   * or a full write that created it).
   */
  private static boolean resurrected(List<Entry> entries, Map<String, Object> actual,
      Timeline timeline) {
    int n = entries.size();
    if (n < 2) {
      return false;
    }
    Entry last = entries.get(n - 1);
    if (last.type() != OpType.DELETE || !onNew(last, timeline)) {
      return false;
    }
    int first = 0;
    while (first < n && !onNew(entries.get(first), timeline)) {
      first++;
    }
    for (int i = first; i < n; i++) {
      // every write after the swap went to the new index before the reconcile could reach it
      if (!onNew(entries.get(i), timeline)
          || entries.get(i).startedAt() >= timeline.reconcileBound()) {
        return false;
      }
    }
    List<Entry> onOld = entries.subList(0, first);
    if (!same(OpLog.replay(onOld), actual)) {
      return false;
    }
    // the new index did not hold the document at the swap: its first write there found nothing
    Entry firstNew = entries.get(first);
    if (firstNew.type() == OpType.UPDATE || firstNew.found()) {
      return false;
    }
    // the reconcile creates the document once, after the last of these writes: a write of the new
    // index that found it while the log held it deleted shows it had already been created again,
    // so a later creation is a second one (for example by a resumed reconcile)
    Map<String, Object> logState = OpLog.replay(entries.subList(0, first + 1));
    for (int i = first + 1; i < n; i++) {
      Entry entry = entries.get(i);
      if (logState == null && entry.found()) {
        return false;
      }
      logState = OpLog.replay(logState, List.of(entry));
    }
    return createdLateOnOld(onOld, timeline);
  }

  /**
   * Finds the stale base a partial update after the swap applied to. The new index held, at the
   * swap, the state the catch-up pass read (writes sent before {@code copiedAt}); when the document
   * was absent then, because it was deleted during the copy, the state the first copy read
   * (before {@code preparedAt}). The writes after that base and before the update are lost.
   */
  private static Anomaly staleUpdate(List<Entry> entries, Map<String, Object> actual,
      Timeline timeline) {
    int k = -1;
    for (int i = 0; i < entries.size(); i++) {
      if (onNew(entries.get(i), timeline)) {
        k = i;
        break;
      }
    }
    if (k < 1 || entries.get(k).type() != OpType.UPDATE
        || entries.get(k).startedAt() >= timeline.reconcileBound()) {
      return null;
    }
    List<Entry> after = entries.subList(k, entries.size());
    // base i: the new index held the state after write i; writes i+1 .. k-1 were lost
    for (int i = k - 2; i >= -1; i--) {
      Entry lost = entries.get(i + 1);
      if (!timeline.oldIndex().equals(lost.index()) || lost.startedAt() < timeline.preparedAt()) {
        return null;
      }
      Map<String, Object> base = OpLog.replay(entries.subList(0, i + 1));
      if (base == null || !same(OpLog.replay(base, after), actual)) {
        continue;
      }
      if (lost.startedAt() >= timeline.copiedAt()) {
        return Anomaly.STALE_PARTIAL_UPDATE;
      }
      // lost writes before the catch-up: only explained when they left the document deleted
      // when the catch-up read the old index, so that it never refreshed the copy
      List<Entry> beforeCatchUp = new ArrayList<>(entries.subList(0, i + 1));
      for (Entry entry : entries.subList(i + 1, k)) {
        if (entry.startedAt() < timeline.copiedAt()) {
          beforeCatchUp.add(entry);
        }
      }
      // and the base is what the first copy read: acknowledged before it started
      if (OpLog.replay(beforeCatchUp) == null && i >= 0
          && entries.get(i).ackedAt() < timeline.preparedAt()) {
        return Anomaly.STALE_PARTIAL_UPDATE_ON_GHOST;
      }
    }
    return null;
  }

  /** Whether two documents are equal as canonical JSON (null means absent). */
  static boolean same(Map<String, Object> a, Map<String, Object> b) {
    if (a == null || b == null) {
      return a == b;
    }
    return Json.canonical(a).equals(Json.canonical(b));
  }

  /** Describes the writes of an id, one line each, times relative to the swap and the copy. */
  public static List<String> describe(List<Entry> entries, Timeline timeline) {
    List<String> lines = new ArrayList<>();
    for (Entry entry : entries) {
      String where = entry.index() == null ? "?"
          : entry.index().equals(timeline.oldIndex()) ? "old" : "new:" + entry.index();
      String when = timeline.swappedAt() > 0
          ? "swap" + signed(entry.startedAt() - timeline.swappedAt()) + "ms"
          : "t=" + entry.startedAt();
      String copied = timeline.copiedAt() != Long.MAX_VALUE
          ? " copied" + signed(entry.startedAt() - timeline.copiedAt()) + "ms" : "";
      lines.add(entry.type() + " " + where + " " + when + copied
          + (entry.type() == OpType.UPDATE ? "" : " found=" + entry.found())
          + (entry.body() == null ? "" : " " + Json.canonical(entry.body())));
    }
    return lines;
  }

  private static String signed(long value) {
    return value >= 0 ? "+" + value : Long.toString(value);
  }
}
