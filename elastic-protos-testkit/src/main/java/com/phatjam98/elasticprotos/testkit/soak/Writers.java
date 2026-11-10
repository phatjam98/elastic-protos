package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.engine.BulkOp;
import com.phatjam98.elasticprotos.engine.BulkResult;
import com.phatjam98.elasticprotos.engine.EngineException;
import com.phatjam98.elasticprotos.engine.IndexOptions;
import com.phatjam98.elasticprotos.engine.IndexResult;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.testkit.soak.OpLog.Entry;
import com.phatjam98.elasticprotos.testkit.soak.OpLog.OpType;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

/**
 * Writer threads that create, overwrite, partially update and delete documents through the alias
 * at a steady rate and record every acknowledged write in the {@link OpLog}.
 *
 * <p>Writer {@code w} of {@code W} owns the seeded ids {@code s-<n>} with {@code n % W == w} and
 * the ids it creates ({@code w<w>-<k>}); no other thread writes them, and a writer sends one
 * request at a time, so the log's order per id is the engine's. A blocked write (and a 429) is
 * retried after a pause, as the README advises; every failed attempt is recorded. A write whose
 * outcome is unknown (timeout, I/O) makes its id uncertain and the writer never touches it again.
 *
 * <p>Ids are chosen with a bias towards the writer's recently written ones, so that the same
 * document is written on both sides of the swap often enough to exercise the reconcile.
 */
public final class Writers {

  /** Ids per writer remembered as recently written. */
  private static final int RECENT = 64;
  /** Ids the readers sample from, written to the old index after the copy began. */
  private static final int LATE_SAMPLE = 20_000;

  private final SearchEngine engine;
  private final String alias;
  private final OpLog log;
  private final WriteFailures failures;
  private final PhaseClock clock;
  private final int count;
  private final double rate;
  private final long seed;
  private final int seeded;
  private final List<Thread> threads = new ArrayList<>();
  private volatile boolean stopping;
  private volatile String oldIndex;
  private final AtomicReferenceArray<String> late = new AtomicReferenceArray<>(LATE_SAMPLE);
  private final AtomicLong lateCount = new AtomicLong();
  private final Map<String, AtomicLong> counters = new TreeMap<>();
  private final List<Observation> observations = new ArrayList<>();

  /**
   * A write whose result contradicted the log's view of the document: a create (the log held it
   * as absent) that found it ({@code createFoundExisting}), an overwrite that created it ({@code
   * indexFoundMissing}), or a delete that found nothing ({@code deleteFoundMissing}).
   *
   * @param id the document id
   * @param kind which contradiction
   * @param phase the phase label when it was acknowledged
   * @param startedAt when it was sent
   * @param at when it was acknowledged
   * @param before the acknowledged writes of the id before this one
   */
  public record Observation(String id, String kind, String phase, long startedAt, long at,
      List<Entry> before) {}

  /**
   * Creates writers.
   *
   * @param engine the engine
   * @param alias the alias to write through
   * @param log the log
   * @param failures where failures go
   * @param clock the phase clock, for labels
   * @param count the number of writer threads
   * @param rate operations per second per writer
   * @param seed the random seed
   * @param seeded the number of seeded ids {@code s-0 .. s-(seeded-1)}
   */
  public Writers(SearchEngine engine, String alias, OpLog log, WriteFailures failures,
      PhaseClock clock, int count, double rate, long seed, int seeded) {
    this.engine = engine;
    this.alias = alias;
    this.log = log;
    this.failures = failures;
    this.clock = clock;
    this.count = count;
    this.rate = rate;
    this.seed = seed;
    this.seeded = seeded;
  }

  /** Returns the id of seeded document {@code n}. */
  public static String seedId(int n) {
    return String.format(Locale.ROOT, "s-%07d", n);
  }

  /** Sets the index the alias pointed at before the migration, to tell late writes apart. */
  public void oldIndex(String index) {
    this.oldIndex = index;
  }

  /** Starts the threads. */
  public void start() {
    for (int w = 0; w < count; w++) {
      int writer = w;
      Thread thread = new Thread(() -> run(writer), "soak-writer-" + w);
      thread.setDaemon(true);
      threads.add(thread);
      thread.start();
    }
  }

  /** Stops the threads after their current write and waits for them. */
  public void stop() throws InterruptedException {
    stopping = true;
    for (Thread thread : threads) {
      thread.join();
    }
  }

  /** Returns a random id written to the old index after the copy began, or null. */
  public String lateId(Random random) {
    long count = lateCount.get();
    if (count == 0) {
      return null;
    }
    return late.get(random.nextInt((int) Math.min(count, LATE_SAMPLE)));
  }

  /** Returns the observations so far. */
  public synchronized List<Observation> observations() {
    return List.copyOf(observations);
  }

  /** Returns the counters: attempts, acknowledged writes by type, observations. */
  public synchronized Map<String, Object> stats() {
    Map<String, Object> stats = new LinkedHashMap<>();
    counters.forEach((key, value) -> stats.put(key, value.get()));
    return stats;
  }

  private synchronized void count(String key) {
    counters.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
  }

  /** The ids one writer owns, live and deleted, and its recently written ones. */
  private static final class Owned {
    final List<String> live = new ArrayList<>();
    final Map<String, Integer> livePos = new HashMap<>();
    final List<String> dead = new ArrayList<>();
    final ArrayDeque<String> recent = new ArrayDeque<>();

    void addLive(String id) {
      if (!livePos.containsKey(id)) {
        livePos.put(id, live.size());
        live.add(id);
      }
    }

    void removeLive(String id) {
      Integer pos = livePos.remove(id);
      if (pos == null) {
        return;
      }
      String last = live.remove(live.size() - 1);
      if (pos < live.size()) {
        live.set(pos, last);
        livePos.put(last, pos);
      }
    }

    void touch(String id) {
      recent.addLast(id);
      if (recent.size() > RECENT) {
        recent.removeFirst();
      }
    }
  }

  private void run(int writer) {
    Random random = new Random(seed * 31 + writer);
    Owned owned = new Owned();
    for (int n = writer; n < seeded; n += count) {
      owned.addLive(seedId(n));
    }
    long next = 0;
    long period = (long) (1_000_000_000L / rate);
    long nextAt = System.nanoTime();
    while (!stopping) {
      nextAt += period;
      long wait = nextAt - System.nanoTime();
      if (wait > 0) {
        LockSupport.parkNanos(wait);
      } else if (wait < -period * 10) {
        nextAt = System.nanoTime();
      }
      double r = random.nextDouble();
      String id;
      OpType type;
      if (owned.live.isEmpty() || r < 0.25) {
        if (!owned.dead.isEmpty() && random.nextBoolean()) {
          id = owned.dead.remove(random.nextInt(owned.dead.size()));
        } else {
          id = "w" + writer + "-" + next++;
        }
        type = OpType.CREATE;
      } else {
        id = pickLive(owned, random);
        type = r < 0.55 ? OpType.INDEX : r < 0.80 ? OpType.UPDATE : OpType.DELETE;
      }
      write(owned, id, type, random);
    }
  }

  private String pickLive(Owned owned, Random random) {
    if (random.nextDouble() < 0.4 && !owned.recent.isEmpty()) {
      List<String> recent = new ArrayList<>(owned.recent);
      for (int i = 0; i < 3; i++) {
        String id = recent.get(random.nextInt(recent.size()));
        if (owned.livePos.containsKey(id)) {
          return id;
        }
      }
    }
    return owned.live.get(random.nextInt(owned.live.size()));
  }

  private void write(Owned owned, String id, OpType type, Random random) {
    long rev = System.nanoTime();
    Map<String, Object> body = type == OpType.UPDATE ? SoakSchema.partial(rev, random)
        : type == OpType.DELETE ? null : SoakSchema.document(id, rev, random);
    while (true) {
      count("attempted." + type);
      List<Entry> before = log.history(id).entries();
      log.begin(id);
      long started = System.currentTimeMillis();
      Outcome outcome = send(id, type, body, started);
      if (outcome.entry != null) {
        log.ack(id, outcome.entry);
        count("acked." + type);
        observe(id, type, outcome.entry, before);
        Entry entry = outcome.entry;
        if (type == OpType.DELETE) {
          owned.removeLive(id);
          owned.dead.add(id);
        } else {
          owned.addLive(id);
        }
        owned.touch(id);
        if (oldIndex != null && oldIndex.equals(entry.index()) && clock.seen("COPIED")) {
          late.set((int) (lateCount.getAndIncrement() % LATE_SAMPLE), id);
        }
        return;
      }
      boolean retry = outcome.retryable && !stopping;
      failures.add(new WriteFailures.Failure(id, type.name(), outcome.kind, outcome.status,
          outcome.message, started, System.currentTimeMillis(), clock.current(), retry));
      if (outcome.unknown) {
        log.unknown(id);
        count("unknownOutcome." + type);
        owned.removeLive(id);
        owned.dead.remove(id);
        return;
      }
      log.rejected(id);
      if (!retry) {
        count("rejected." + type);
        if (type == OpType.CREATE) {
          owned.dead.add(id);
        }
        return;
      }
      count("retried." + type);
      LockSupport.parkNanos(100_000_000L);
    }
  }

  /** Records writes whose result contradicts the log's view of the document. */
  private void observe(String id, OpType type, Entry entry, List<Entry> before) {
    String kind = type == OpType.CREATE && entry.found() ? "createFoundExisting"
        : type == OpType.INDEX && !entry.found() ? "indexFoundMissing"
            : type == OpType.DELETE && !entry.found() ? "deleteFoundMissing" : null;
    if (kind == null) {
      return;
    }
    String phase = clock.current();
    count("observed." + kind + "." + phase);
    synchronized (this) {
      observations.add(new Observation(id, kind, phase, entry.startedAt(), entry.ackedAt(),
          before));
    }
  }

  /** The result of one attempt. */
  private static final class Outcome {
    Entry entry;
    String kind;
    int status;
    String message;
    boolean retryable;
    boolean unknown;
  }

  private Outcome send(String id, OpType type, Map<String, Object> body, long started) {
    Outcome outcome = new Outcome();
    try {
      switch (type) {
        case DELETE:
          delete(id, started, outcome);
          return outcome;
        case UPDATE:
          IndexResult updated = engine.update(alias, id, body);
          outcome.entry = new Entry(type, body, started, System.currentTimeMillis(),
              updated.index(), true);
          return outcome;
        default:
          IndexResult written = engine.index(alias, id, body, IndexOptions.defaults());
          outcome.entry = new Entry(type, body, started, System.currentTimeMillis(),
              written.index(), "updated".equals(written.result()));
          return outcome;
      }
    } catch (EngineException e) {
      outcome.kind = e.getKind().name();
      outcome.status = e.getStatus();
      outcome.message = shorten(e.getMessage());
      switch (e.getKind()) {
        case BLOCKED:
          outcome.retryable = true;
          break;
        case TRANSIENT:
          outcome.retryable = e.getStatus() == 429;
          outcome.unknown = e.getStatus() != 429;
          break;
        case NOT_FOUND:
        case CONFLICT:
        case BAD_REQUEST:
        case UNAUTHORIZED:
        case ALREADY_EXISTS:
          break;
        default:
          outcome.unknown = true;
          break;
      }
      return outcome;
    } catch (RuntimeException e) {
      outcome.kind = e.getClass().getSimpleName();
      outcome.message = shorten(String.valueOf(e.getMessage()));
      outcome.unknown = true;
      return outcome;
    }
  }

  /** Deletes through bulk, whose item names the concrete index (the delete API does not). */
  private void delete(String id, long started, Outcome outcome) {
    BulkResult result = engine.bulk(List.of(BulkOp.delete(alias, id)));
    BulkResult.Item item = result.items().get(0);
    if (item.ok()) {
      outcome.entry = new Entry(OpType.DELETE, null, started, System.currentTimeMillis(),
          item.index(), item.status() != 404);
      return;
    }
    String error = item.error() == null ? "" : item.error();
    outcome.status = item.status();
    outcome.message = shorten(error);
    outcome.kind = error.startsWith("cluster_block_exception") ? "BLOCKED"
        : item.status() == 409 ? "CONFLICT" : item.status() == 404 ? "NOT_FOUND"
            : item.status() == 429 ? "TRANSIENT" : "OTHER";
    outcome.retryable = "BLOCKED".equals(outcome.kind) || item.status() == 429;
    outcome.unknown = !outcome.retryable && item.status() >= 500;
  }

  private static String shorten(String message) {
    return message == null || message.length() <= 300 ? message : message.substring(0, 300);
  }
}
