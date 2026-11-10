package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.testkit.soak.LogComparison.Timeline;
import com.phatjam98.elasticprotos.testkit.soak.OpLog.Entry;
import com.phatjam98.elasticprotos.testkit.soak.WriteFailures.Mode;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
      return new Timeline(oldIndex, Long.MAX_VALUE, Long.MAX_VALUE, swappedAt, false);
    }
    long copiedAt = swappedAt > 0 ? clock.lastBefore("COPIED", swappedAt) : -1;
    // a ghost (a document deleted during a copy) can date from the first copy into a reused index
    long preparedAt = swappedAt > 0 ? clock.first("PREPARED") : -1;
    return new Timeline(oldIndex, preparedAt < 0 ? Long.MAX_VALUE : preparedAt,
        copiedAt < 0 ? Long.MAX_VALUE : copiedAt, swappedAt, true);
  }

  /**
   * Compares the alias with the log after the writers stopped and summarises writes, failures and
   * reads.
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
    long reconciledAt;
    switch (mode) {
      case ZERO_DOWNTIME:
        swapNotBefore = timeline.swappedAt() > 0
            ? clock.lastBefore("SNAPSHOT", timeline.swappedAt()) : -1;
        reconciledAt = clock.last("RECONCILED");
        break;
      case BLOCKING:
        swapNotBefore = clock.last("COPIED");
        reconciledAt = timeline.swappedAt() > 0 ? timeline.swappedAt() : -1;
        break;
      default:
        swapNotBefore = -1;
        reconciledAt = -1;
        break;
    }
    Map<String, Object> reads = reader.summarize(swapNotBefore,
        timeline.swappedAt() > 0 ? timeline.swappedAt() : -1, reconciledAt,
        mode == Mode.ZERO_DOWNTIME ? clock.last("SWEPT") : reconciledAt, comparison.differing());
    final long staleBeforeSwap = (Long) reads.get("staleBeforeSwap");

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
    Map<String, Object> writeSummary = new LinkedHashMap<>();
    writeSummary.put("attempts", attempted);
    writeSummary.put("acknowledged", acked);
    writeSummary.put("counters", writes);
    section.put("writes", writeSummary);
    section.put("failures", failureSummary);
    section.put("reads", reads);
    section.put("comparison", comparison.toMap());
    long violations = comparison.violationCount() + (Long) failureSummary.get("violations")
        + staleBeforeSwap;
    section.put("violations", violations);
    return section;
  }
}
