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
  private final List<Object> samples = new ArrayList<>();
  private long discarded;
  private long errors;

  /** A stale read. */
  private record StaleRead(String id, long started, long at) {}

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
          stale.add(new StaleRead(id, started, at));
          if (samples.size() < 20) {
            Map<String, Object> sample = new LinkedHashMap<>();
            sample.put("id", id);
            sample.put("phase", phase);
            sample.put("at", at);
            sample.put("expected", view.expected());
            sample.put("actual", actual);
            samples.add(sample);
          }
        }
      }
      LockSupport.parkNanos(2_000_000L);
    }
  }

  /**
   * Summarises the reads.
   *
   * @param swapNotBefore a time before which the alias still pointed at the old index for sure
   *     (the snapshot phase of a zero-downtime reindex, the copy of a blocking one), or -1 when the
   *     alias never moves; stale reads that ended before it are counted as {@code
   *     staleBeforeSwap}, which no documented behaviour explains
   * @param swappedAt when the swap was reported, or -1
   * @param reconciledAt when the reconcile was reported, or -1
   * @param sweptAt when the sweep was reported, or -1
   * @param persistent ids that still differ from the log at the end (documented anomalies or
   *     violations); their stale reads are counted apart and left out of the window
   * @return the summary
   */
  public synchronized Map<String, Object> summarize(long swapNotBefore, long swappedAt,
      long reconciledAt, long sweptAt, java.util.Set<String> persistent) {
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
    long first = Long.MAX_VALUE;
    long last = Long.MIN_VALUE;
    long beforeSwap = 0;
    long afterReconcile = 0;
    long afterSweep = 0;
    long ofPersistent = 0;
    for (StaleRead read : stale) {
      if (swapNotBefore < 0 || read.at() < swapNotBefore) {
        beforeSwap++;
      }
      if (persistent.contains(read.id())) {
        ofPersistent++;
        continue;
      }
      first = Math.min(first, read.started());
      last = Math.max(last, read.at());
      if (reconciledAt >= 0 && read.started() > reconciledAt) {
        afterReconcile++;
      }
      if (sweptAt >= 0 && read.started() > sweptAt) {
        afterSweep++;
      }
    }
    summary.put("reads", reads);
    summary.put("discardedConcurrentWrite", discarded);
    summary.put("errors", errors);
    summary.put("stale", staleCount);
    summary.put("byPhase", phases);
    summary.put("staleBeforeSwap", beforeSwap);
    summary.put("staleOfPersistentIds", ofPersistent);
    summary.put("staleAfterReconcile", afterReconcile);
    summary.put("staleAfterSweep", afterSweep);
    if (first != Long.MAX_VALUE && swappedAt > 0) {
      summary.put("firstStaleAfterSwapMs", first - swappedAt);
      summary.put("lastStaleAfterSwapMs", last - swappedAt);
      summary.put("observedStaleWindowMs", last - first);
    }
    if (swappedAt > 0 && reconciledAt > 0) {
      summary.put("swapToReconciledMs", reconciledAt - swappedAt);
    }
    summary.put("samples", samples);
    return summary;
  }
}
