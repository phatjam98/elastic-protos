package com.phatjam98.elasticprotos.migration;

import com.phatjam98.elasticprotos.engine.EngineException;
import com.phatjam98.elasticprotos.engine.Hit;
import com.phatjam98.elasticprotos.engine.IndexOptions;
import com.phatjam98.elasticprotos.engine.IndexResult;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-alias migration locks, kept as one document per alias in a lock index.
 *
 * <p>A lock is taken by creating the document {@code <lock index>/<alias>} ({@code op_type:
 * create}); only one creator wins. Its holder renews it on a daemon thread every third of the TTL
 * with a write conditional on the sequence number of its previous write, and releases it with a
 * conditional delete. A document whose {@code expires_at} has passed belongs to a dead holder: it
 * is deleted on the sequence number it was read with, so of several processes taking it over at
 * once exactly one deletes it, and all of them then race to create it again.
 *
 * <p>The lock index is created on first use with one shard and a strict mapping. Clocks of the
 * processes must agree to well within the TTL.
 */
final class AliasLock {

  private static final Logger LOG = LoggerFactory.getLogger(AliasLock.class);

  /** Attempts to read or create the lock document before giving up on a misbehaving index. */
  private static final int MAX_MISSING = 5;

  private final SearchEngine engine;
  private final String lockIndex;
  private final Duration ttl;
  private final IntSupplier replicas;
  private final String holder;
  private volatile boolean indexReady;

  /**
   * Creates the locks of one process.
   *
   * @param engine the engine
   * @param lockIndex the lock index name
   * @param ttl how long a lock lives without a heartbeat
   * @param replicas the replica count of the lock index, asked for when it is created
   */
  AliasLock(SearchEngine engine, String lockIndex, Duration ttl, IntSupplier replicas) {
    this.engine = engine;
    this.lockIndex = lockIndex;
    this.ttl = ttl;
    this.replicas = replicas;
    this.holder = host() + "/" + ProcessHandle.current().pid() + "/"
        + UUID.randomUUID().toString().substring(0, 8);
  }

  /** Returns this process's holder id, {@code hostname/pid/uuid8}. */
  String holder() {
    return holder;
  }

  /** Returns the lock index. */
  String lockIndex() {
    return lockIndex;
  }

  /**
   * Takes the lock of an alias, waiting while another live holder has it.
   *
   * @param alias the alias
   * @param action what the holder is about to do, recorded for operators
   * @param hash the target hash, recorded for operators
   * @param wait how long to wait for another holder; zero fails at once
   * @return the lease, to be closed when done
   * @throws LockException when another holder still has the lock after {@code wait}
   */
  Lease acquire(String alias, String action, String hash, Duration wait) {
    Instant deadline = Instant.now().plus(wait);
    int missing = 0;
    while (true) {
      ensureIndex();
      Instant now = now();
      try {
        IndexResult created = engine.index(lockIndex, alias, document(alias, action, hash, now),
            new IndexOptions(true, IndexOptions.Refresh.IMMEDIATE));
        LOG.info("{}: took the migration lock as {}", alias, holder);
        return new Lease(alias, action, hash, now, created);
      } catch (EngineException e) {
        if (e.getKind() == EngineException.Kind.NOT_FOUND && ++missing <= MAX_MISSING) {
          indexReady = false;
          continue;
        }
        if (e.getKind() != EngineException.Kind.CONFLICT) {
          throw e;
        }
      }
      Optional<Hit> held = engine.get(lockIndex, alias);
      if (held.isEmpty()) {
        continue;
      }
      Hit lock = held.get();
      String other = string(lock.source().get("holder"));
      if (expired(lock)) {
        LOG.warn("{}: taking over the migration lock of {}, which expired at {}", alias, other,
            lock.source().get("expires_at"));
        try {
          engine.delete(lockIndex, alias, IndexOptions.ifMatch(lock.seqNo(), lock.primaryTerm())
              .withRefresh(IndexOptions.Refresh.IMMEDIATE));
        } catch (EngineException e) {
          if (e.getKind() != EngineException.Kind.CONFLICT
              && e.getKind() != EngineException.Kind.NOT_FOUND) {
            throw e;
          }
          // another process took it over first; try again
        }
        continue;
      }
      Duration left = Duration.between(Instant.now(), deadline);
      if (left.isNegative() || left.isZero()) {
        throw new LockException("the migration lock of " + alias + " is held by " + other
            + " (" + lock.source().get("action") + ", expires at "
            + lock.source().get("expires_at") + ")", other, false);
      }
      long pause = Math.min(left.toMillis(), ThreadLocalRandom.current().nextLong(1000, 2001));
      LOG.info("{}: waiting for the migration lock held by {}", alias, other);
      try {
        Thread.sleep(pause);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new LockException("interrupted while waiting for the migration lock of " + alias,
            other, false);
      }
    }
  }

  private void ensureIndex() {
    if (indexReady) {
      return;
    }
    if (!engine.indexExists(lockIndex)) {
      Map<String, Object> keyword = Map.of("type", "keyword");
      Map<String, Object> date = Map.of("type", "date");
      Map<String, Object> properties = new LinkedHashMap<>();
      properties.put("alias", keyword);
      properties.put("holder", keyword);
      properties.put("action", keyword);
      properties.put("hash", keyword);
      properties.put("acquired_at", date);
      properties.put("expires_at", date);
      Map<String, Object> settings = new LinkedHashMap<>();
      settings.put("number_of_shards", 1);
      settings.put("number_of_replicas", replicas.getAsInt());
      try {
        engine.createIndex(lockIndex, settings,
            Map.of("dynamic", "strict", "properties", properties), Set.of());
        LOG.info("created the migration lock index {}", lockIndex);
      } catch (EngineException e) {
        if (e.getKind() != EngineException.Kind.ALREADY_EXISTS) {
          throw e;
        }
      }
    }
    indexReady = true;
  }

  private boolean expired(Hit lock) {
    try {
      return Instant.parse(string(lock.source().get("expires_at"))).isBefore(Instant.now());
    } catch (DateTimeParseException | NullPointerException e) {
      return true;
    }
  }

  private Map<String, Object> document(String alias, String action, String hash,
      Instant acquired) {
    Map<String, Object> doc = new LinkedHashMap<>();
    doc.put("alias", alias);
    doc.put("holder", holder);
    doc.put("action", action);
    doc.put("hash", hash);
    doc.put("acquired_at", acquired.toString());
    doc.put("expires_at", now().plus(ttl).toString());
    return doc;
  }

  private static Instant now() {
    return Instant.now().truncatedTo(ChronoUnit.MILLIS);
  }

  private static String string(Object value) {
    return value == null ? null : value.toString();
  }

  private static String host() {
    String env = System.getenv("HOSTNAME");
    if (env != null && !env.isBlank()) {
      return env;
    }
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return "unknown-host";
    }
  }

  /**
   * A held lock. It is renewed in the background until it is closed; {@link #check()} throws once
   * a renewal found the lock taken over or deleted.
   */
  final class Lease implements AutoCloseable {

    private final String alias;
    private final String action;
    private final String hash;
    private final Instant acquired;
    private final ScheduledExecutorService heartbeat;
    private long seqNo;
    private long primaryTerm;
    private volatile String lostReason;
    private boolean closed;

    private Lease(String alias, String action, String hash, Instant acquired,
        IndexResult created) {
      this.alias = alias;
      this.action = action;
      this.hash = hash;
      this.acquired = acquired;
      this.seqNo = created.seqNo() == null ? 0 : created.seqNo();
      this.primaryTerm = created.primaryTerm() == null ? 1 : created.primaryTerm();
      this.heartbeat = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "elastic-protos-lock-" + alias);
        thread.setDaemon(true);
        return thread;
      });
      long period = Math.max(1, ttl.toMillis() / 3);
      heartbeat.scheduleAtFixedRate(this::heartbeat, period, period, TimeUnit.MILLISECONDS);
    }

    /** Returns the alias. */
    String alias() {
      return alias;
    }

    /**
     * Renews the lock now: rewrites its document with a later {@code expires_at}, conditional on
     * the sequence number of the previous write. Marks the lease lost when the document was taken
     * over or deleted; other errors are logged and retried at the next beat.
     */
    synchronized void heartbeat() {
      if (closed || lostReason != null) {
        return;
      }
      try {
        IndexResult renewed = engine.index(lockIndex, alias,
            document(alias, action, hash, acquired),
            IndexOptions.ifMatch(seqNo, primaryTerm).withRefresh(IndexOptions.Refresh.IMMEDIATE));
        seqNo = renewed.seqNo() == null ? seqNo : renewed.seqNo();
        primaryTerm = renewed.primaryTerm() == null ? primaryTerm : renewed.primaryTerm();
      } catch (EngineException e) {
        if (e.getKind() == EngineException.Kind.CONFLICT
            || e.getKind() == EngineException.Kind.NOT_FOUND) {
          lostReason = e.getMessage();
          LOG.warn("{}: lost the migration lock: {}", alias, e.getMessage());
        } else {
          LOG.warn("{}: could not renew the migration lock: {}", alias, e.getMessage());
        }
      } catch (RuntimeException e) {
        LOG.warn("{}: could not renew the migration lock: {}", alias, e.getMessage());
      }
    }

    /**
     * Throws when the lock was lost.
     *
     * @throws LockException when a renewal found the lock taken over or deleted
     */
    void check() {
      if (lostReason != null) {
        throw new LockException("the migration lock of " + alias + " was lost: " + lostReason
            + "; the next run resumes", holder, true);
      }
    }

    /** Stops the renewals and deletes the lock document unless it was lost. */
    @Override
    public synchronized void close() {
      if (closed) {
        return;
      }
      closed = true;
      heartbeat.shutdownNow();
      if (lostReason != null) {
        return;
      }
      try {
        engine.delete(lockIndex, alias, IndexOptions.ifMatch(seqNo, primaryTerm)
            .withRefresh(IndexOptions.Refresh.IMMEDIATE));
        LOG.info("{}: released the migration lock", alias);
      } catch (EngineException e) {
        if (e.getKind() != EngineException.Kind.CONFLICT
            && e.getKind() != EngineException.Kind.NOT_FOUND) {
          LOG.warn("{}: could not release the migration lock (it expires on its own): {}",
              alias, e.getMessage());
        }
      } catch (RuntimeException e) {
        LOG.warn("{}: could not release the migration lock (it expires on its own): {}", alias,
            e.getMessage());
      }
    }
  }
}
