package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.engine.Hit;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.locks.LockSupport;

/**
 * Samples realtime reads ({@code GET /<alias>/_doc/<id>}) during a run and compares each with the
 * operation log. A read counts only when no write of the id was in flight and none started or
 * ended while it ran, so the expected document is well defined. A read that differs from the log
 * is stale.
 *
 * <p>Half the reads target ids the old index acknowledged after the copy began (the ones the
 * catch-up may have missed), the other half seeded ids.
 */
public final class StaleReader {

  private final SearchEngine engine;
  private final String alias;
  private final OpLog log;
  private final Writers writers;
  private final PhaseClock clock;
  private final int seeded;
  private final long seed;
  private Thread thread;
  private volatile boolean stopping;
  private final Map<String, long[]> byPhase = new TreeMap<>();
  private final List<StaleRead> stale = new ArrayList<>();
  private long discarded;
  private long errors;

  /** A stale read, with the id's writes when it was read. */
  private record StaleRead(String id, long started, long at, String phase,
      Map<String, Object> expected, Map<String, Object> actual, List<OpLog.Entry> history) {}

  /**
   * Creates a reader.
   *
   * @param engine the engine
   * @param alias the alias to read through
   * @param log the log
   * @param writers the writers, for ids written late to the old index
   * @param clock the phase clock
   * @param seeded the number of seeded ids
   * @param seed the random seed
   */
  public StaleReader(SearchEngine engine, String alias, OpLog log, Writers writers,
      PhaseClock clock, int seeded, long seed) {
    this.engine = engine;
    this.alias = alias;
    this.log = log;
    this.writers = writers;
    this.clock = clock;
    this.seeded = seeded;
    this.seed = seed;
  }

  /** Starts sampling. */
  public void start() {
    thread = new Thread(this::run, "soak-reader");
    thread.setDaemon(true);
    thread.start();
  }

  /** Stops sampling and waits. */
  public void stop() throws InterruptedException {
    stopping = true;
    thread.join();
  }

  private void run() {
    Random random = new Random(seed * 17 + 5);
    while (!stopping) {
      String id = random.nextBoolean() ? writers.lateId(random) : null;
      if (id == null) {
        if (seeded == 0) {
          LockSupport.parkNanos(10_000_000L);
          continue;
        }
        id = Writers.seedId(random.nextInt(seeded));
      }
      OpLog.View view = log.view(id);
      if (view == null) {
        continue;
      }
      long started = System.currentTimeMillis();
      Optional<Hit> hit;
      try {
        hit = engine.get(alias, id);
      } catch (RuntimeException e) {
        synchronized (this) {
          errors++;
        }
        continue;
      }
      long at = System.currentTimeMillis();
      String phase = clock.current();
      if (log.changedSince(id, view)) {
        synchronized (this) {
          discarded++;
        }
        continue;
      }
      Map<String, Object> actual = hit.map(Hit::source).orElse(null);
      boolean fresh = LogComparison.same(view.expected(), actual);
      synchronized (this) {
        long[] counts = byPhase.computeIfAbsent(phase, key -> new long[2]);
        counts[0]++;
        if (!fresh) {
          counts[1]++;
          stale.add(new StaleRead(id, started, at, phase, view.expected(), actual,
              view.entries()));
        }
      }
      LockSupport.parkNanos(2_000_000L);
    }
  }

  /**
   * Summarises the reads and classifies every stale one ({@link TransientChecks#read}).
   *
   * @param mode the migration mode
   * @param timeline the migration timeline
   * @param swapNotBefore before this the alias pointed at the old index for sure (the snapshot
   *     phase of a zero-downtime reindex, the copy of a blocking one), or -1 when it never moves
   * @return the summary; {@code violations} counts the stale reads no documented behaviour
   *     explains
   */
  public synchronized Map<String, Object> summarize(WriteFailures.Mode mode,
      LogComparison.Timeline timeline, long swapNotBefore) {
    Map<String, Object> summary = new LinkedHashMap<>();
    long reads = 0;
    long staleCount = 0;
    Map<String, Object> phases = new LinkedHashMap<>();
    for (Map.Entry<String, long[]> entry : byPhase.entrySet()) {
      reads += entry.getValue()[0];
      staleCount += entry.getValue()[1];
      phases.put(entry.getKey(), Map.of("reads", entry.getValue()[0], "stale",
          entry.getValue()[1]));
    }
    long swappedAt = timeline.swappedAt();
    long first = Long.MAX_VALUE;
    long last = Long.MIN_VALUE;
    Map<String, Long> byClass = new TreeMap<>();
    Map<String, List<Object>> samples = new TreeMap<>();
    long violations = 0;
    for (StaleRead read : stale) {
      String kind = TransientChecks.read(mode, timeline, swapNotBefore, read.started(),
          read.at(), read.history(), read.actual());
      byClass.merge(kind, 1L, Long::sum);
      if (TransientChecks.VIOLATION.equals(kind)) {
        violations++;
      } else if ("RECONCILE_WINDOW".equals(kind) || "GHOST_BEFORE_SWEEP".equals(kind)) {
        first = Math.min(first, read.started());
        last = Math.max(last, read.at());
      }
      List<Object> list = samples.computeIfAbsent(kind, k -> new ArrayList<>());
      if (list.size() < 10) {
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("id", read.id());
        sample.put("phase", read.phase());
        sample.put("atSwapMs", swappedAt > 0 ? read.at() - swappedAt : null);
        sample.put("expected", read.expected());
        sample.put("actual", read.actual());
        sample.put("history", LogComparison.describe(read.history(), timeline));
        list.add(sample);
      }
    }
    summary.put("reads", reads);
    summary.put("discardedConcurrentWrite", discarded);
    summary.put("errors", errors);
    summary.put("stale", staleCount);
    summary.put("byPhase", phases);
    summary.put("byClass", byClass);
    summary.put("violations", violations);
    if (first != Long.MAX_VALUE && swappedAt > 0) {
      summary.put("firstStaleAfterSwapMs", first - swappedAt);
      summary.put("lastStaleAfterSwapMs", last - swappedAt);
    }
    if (swappedAt > 0 && timeline.reconcileBound() != Long.MAX_VALUE) {
      summary.put("swapToReconcileBoundMs", timeline.reconcileBound() - swappedAt);
    }
    if (swappedAt > 0 && timeline.sweptAt() != Long.MAX_VALUE) {
      summary.put("swapToSweptMs", timeline.sweptAt() - swappedAt);
    }
    summary.put("samples", samples);
    return summary;
  }
}
