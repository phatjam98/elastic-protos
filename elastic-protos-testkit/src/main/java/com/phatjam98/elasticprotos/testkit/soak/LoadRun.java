package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.testkit.soak.LogComparison.Timeline;
import com.phatjam98.elasticprotos.testkit.soak.OpLog.Entry;
import com.phatjam98.elasticprotos.testkit.soak.WriteFailures.Mode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Writers, a stale reader and the operation log around one migration, and the final comparison of
 * the alias with the log.
 */
final class LoadRun {

  final SearchEngine engine;
  final String alias;
  final String oldIndex;
  final OpLog log = new OpLog();
  final WriteFailures failures = new WriteFailures();
  final PhaseClock clock = new PhaseClock();
  final Writers writers;
  final StaleReader reader;
  final int seeded;
  private long seedMs;

  /** Set when the run was killed while reconciling; reported only. */
  boolean reconcileInterrupted;

  LoadRun(SearchEngine engine, String alias, String oldIndex, SoakOptions options, int seeded) {
    this.engine = engine;
    this.alias = alias;
    this.oldIndex = oldIndex;
    this.seeded = seeded;
    this.writers = new Writers(engine, alias, log, failures, clock, options.writers,
        options.rate, options.seed, seeded);
    this.writers.oldIndex(oldIndex);
    this.reader = new StaleReader(engine, alias, log, writers, clock, seeded, options.seed);
  }

  /** Seeds the documents and records them. */
  void seed(long seed) {
    long started = System.currentTimeMillis();
    Cluster.seed(engine, alias, seeded, log, seed);
    seedMs = System.currentTimeMillis() - started;
  }

  void start() {
    writers.start();
    reader.start();
  }

  void stop() throws InterruptedException {
    writers.stop();
    reader.stop();
  }

  /** Returns the timeline of the migration as the clock saw it. */
  Timeline timeline(Mode mode) {
    long swappedAt = Math.max(0, clock.last("SWAPPED"));
    if (mode != Mode.ZERO_DOWNTIME) {
      return Timeline.strict(oldIndex, swappedAt);
    }
    long copiedAt = swappedAt > 0 ? clock.lastBefore("COPIED", swappedAt) : -1;
    long caughtUpAt = swappedAt > 0 ? clock.lastBefore("CAUGHT_UP", swappedAt) : -1;
    long snapshotAt = swappedAt > 0 ? clock.lastBefore("SNAPSHOT", swappedAt) : -1;
    // a ghost (a document deleted during a copy) can date from the first copy into a reused index
    long preparedAt = swappedAt > 0 ? clock.first("PREPARED") : -1;
    // the first reconcile that completed; when a run died while reconciling, the resumed one
    long bound = clock.first("RECONCILED");
    long sweptAt = clock.first("SWEPT");
    return new Timeline(oldIndex, preparedAt < 0 ? Long.MAX_VALUE : preparedAt,
        copiedAt < 0 ? Long.MAX_VALUE : copiedAt, caughtUpAt < 0 ? Long.MAX_VALUE : caughtUpAt,
        snapshotAt < 0 ? swappedAt : snapshotAt, swappedAt,
        bound < 0 ? Long.MAX_VALUE : bound, sweptAt < 0 ? Long.MAX_VALUE : sweptAt, true)
        .withReplayResidual(replayResidual);
  }

  /**
   * The ids a resumed reconcile may create a second time as the documented replay residual; set by
   * the kill scenario from the marker at the kill, none otherwise.
   */
  java.util.function.Predicate<String> replayResidual = id -> false;

  /**
   * Compares the alias with the log after the writers stopped and summarises writes, failures,
   * reads and observations.
   *
   * @param mode the migration mode
   * @param origin the time phases are reported relative to
   * @return the report section; {@code violations} is the number of genuine violations found
   */
  Map<String, Object> verify(Mode mode, long origin) {
    Timeline timeline = timeline(mode);
    Map<String, List<Entry>> snapshot = log.snapshot();
    Map<String, Map<String, Object>> actual = Cluster.scan(engine, alias);
    final LogComparison.Result comparison = LogComparison.compare(snapshot,
        log.uncertainIds(), actual, timeline, 10);
    final Map<String, Object> failureSummary = failures.summarize(mode, timeline, snapshot, 20);
    long swapNotBefore;
    switch (mode) {
      case ZERO_DOWNTIME:
        swapNotBefore = timeline.swappedAt() > 0 ? timeline.snapshotAt() : -1;
        break;
      case BLOCKING:
        swapNotBefore = clock.last("COPIED");
        break;
      default:
        swapNotBefore = -1;
        break;
    }
    final Map<String, Object> reads = reader.summarize(mode, timeline, swapNotBefore);
    final Map<String, Object> observed = observations(mode, timeline, swapNotBefore);

    Map<String, Object> writes = writers.stats();
    long attempted = 0;
    long acked = 0;
    for (Map.Entry<String, Object> entry : writes.entrySet()) {
      if (entry.getKey().startsWith("attempted.")) {
        attempted += (Long) entry.getValue();
      } else if (entry.getKey().startsWith("acked.")) {
        acked += (Long) entry.getValue();
      }
    }
    Map<String, Object> section = new LinkedHashMap<>();
    section.put("seeded", seeded);
    section.put("seedMs", seedMs);
    section.put("documentsAtEnd", actual.size());
    section.put("phases", clock.toList(origin));
    Map<String, Object> bounds = new LinkedHashMap<>();
    bounds.put("copiedAtSwapMs", relative(timeline.copiedAt(), timeline));
    bounds.put("reconcileBoundSwapMs", relative(timeline.reconcileBound(), timeline));
    bounds.put("sweptAtSwapMs", relative(timeline.sweptAt(), timeline));
    bounds.put("reconcileInterrupted", reconcileInterrupted);
    section.put("timeline", bounds);
    Map<String, Object> writeSummary = new LinkedHashMap<>();
    writeSummary.put("attempts", attempted);
    writeSummary.put("acknowledged", acked);
    writeSummary.put("counters", writes);
    section.put("writes", writeSummary);
    section.put("failures", failureSummary);
    section.put("reads", reads);
    section.put("observations", observed);
    section.put("comparison", comparison.toMap());
    long violations = comparison.violationCount() + (Long) failureSummary.get("violations")
        + (Long) reads.get("violations") + (Long) observed.get("violations");
    section.put("violations", violations);
    return section;
  }

  private static Long relative(long time, Timeline timeline) {
    return time == Long.MAX_VALUE || timeline.swappedAt() <= 0 ? null
        : time - timeline.swappedAt();
  }

  /** Classifies the writers' observations ({@link TransientChecks#observation}). */
  private Map<String, Object> observations(Mode mode, Timeline timeline, long swapNotBefore) {
    Map<String, Long> byClass = new TreeMap<>();
    Map<String, List<Object>> samples = new TreeMap<>();
    long violations = 0;
    for (Writers.Observation observation : writers.observations()) {
      String kind = TransientChecks.observation(mode, timeline, swapNotBefore, observation);
      String key = observation.kind() + "/" + kind;
      byClass.merge(key, 1L, Long::sum);
      if (TransientChecks.VIOLATION.equals(kind)) {
        violations++;
      }
      List<Object> list = samples.computeIfAbsent(key, k -> new ArrayList<>());
      if (list.size() < 10 && !"RECONCILE_WINDOW".equals(kind)) {
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("id", observation.id());
        sample.put("phase", observation.phase());
        sample.put("atSwapMs", timeline.swappedAt() > 0
            ? observation.at() - timeline.swappedAt() : null);
        sample.put("historyBefore", LogComparison.describe(observation.before(), timeline));
        list.add(sample);
      }
    }
    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("byClass", byClass);
    summary.put("violations", violations);
    summary.put("samples", samples);
    return summary;
  }
}
