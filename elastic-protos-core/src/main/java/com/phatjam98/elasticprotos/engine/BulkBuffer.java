package com.phatjam98.elasticprotos.engine;

import com.phatjam98.elasticprotos.util.Json;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects {@link BulkOp}s and sends them to a {@link SearchEngine} in batches, when a batch
 * reaches a number of operations or an approximate size, when a flush interval elapses, on an
 * explicit {@link #flush()} and on {@link #close()}.
 *
 * <p>Thread-safe: any thread may add operations. Batches are sent one at a time, in the order the
 * operations were added; a thread that fills a batch sends it before {@link #add} returns, which
 * gives natural back-pressure.
 *
 * <p>Batch outcomes go to the result listener; item failures are reported there, inside the
 * {@link BulkResult}. When the engine call itself throws, the batch is dropped and the exception
 * goes to the error listener, which by default logs it; operations are never retried.
 *
 * <pre>{@code
 * try (BulkBuffer buffer = BulkBuffer.builder()
 *     .engine(engine)
 *     .maxActions(1000)
 *     .flushInterval(Duration.ofSeconds(1))
 *     .listener(result -> result.failures().forEach(System.err::println))
 *     .build()) {
 *   documents.forEach(doc -> buffer.add(BulkOp.index("documents", doc.id(), doc.body())));
 * }
 * }</pre>
 */
public final class BulkBuffer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(BulkBuffer.class);

  /** Bytes added to each operation's estimated size for its action line. */
  static final int ACTION_OVERHEAD = 64;

  private final SearchEngine engine;
  private final int maxActions;
  private final long maxBytes;
  private final Consumer<BulkResult> listener;
  private final Consumer<Throwable> errorListener;
  private final ScheduledExecutorService scheduler;
  private final Object lock = new Object();
  private List<BulkOp> pending = new ArrayList<>();
  private long pendingBytes;
  private boolean closed;

  private BulkBuffer(Builder builder) {
    this.engine = Objects.requireNonNull(builder.engine, "engine");
    this.maxActions = builder.maxActions;
    this.maxBytes = builder.maxBytes;
    this.listener = builder.listener;
    this.errorListener = builder.errorListener;
    Duration interval = builder.flushInterval;
    if (interval != null && !interval.isZero() && !interval.isNegative()) {
      this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "elastic-protos-bulk-flush");
        thread.setDaemon(true);
        return thread;
      });
      long millis = Math.max(1, interval.toMillis());
      scheduler.scheduleWithFixedDelay(this::scheduledFlush, millis, millis,
          TimeUnit.MILLISECONDS);
    } else {
      this.scheduler = null;
    }
  }

  /** Starts building a buffer. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Adds an operation, sending the batch if it is now full.
   *
   * @param op the operation
   * @throws IllegalStateException if the buffer is closed
   */
  public void add(BulkOp op) {
    Objects.requireNonNull(op, "op");
    synchronized (lock) {
      if (closed) {
        throw new IllegalStateException("BulkBuffer is closed");
      }
      pending.add(op);
      pendingBytes += estimate(op);
      if (pending.size() >= maxActions || pendingBytes >= maxBytes) {
        send();
      }
    }
  }

  /** Returns the number of operations waiting to be sent. */
  public int pending() {
    synchronized (lock) {
      return pending.size();
    }
  }

  /**
   * Sends the waiting operations now and waits for the engine.
   *
   * @return the result, or null when nothing was waiting or the engine call failed (the failure
   *     goes to the error listener)
   */
  public BulkResult flush() {
    synchronized (lock) {
      return send();
    }
  }

  /** Stops the flush timer and sends the waiting operations. Later {@link #add}s fail. */
  @Override
  public void close() {
    synchronized (lock) {
      if (closed) {
        return;
      }
      closed = true;
    }
    if (scheduler != null) {
      scheduler.shutdown();
      try {
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    flush();
  }

  private void scheduledFlush() {
    try {
      flush();
    } catch (RuntimeException e) {
      // A throwing listener must not stop the timer.
      LOG.error("Scheduled bulk flush failed", e);
    }
  }

  /** Sends the pending batch; callers hold {@code lock}. */
  private BulkResult send() {
    if (pending.isEmpty()) {
      return null;
    }
    List<BulkOp> batch = pending;
    pending = new ArrayList<>();
    pendingBytes = 0;
    BulkResult result;
    try {
      result = engine.bulk(batch);
    } catch (RuntimeException e) {
      errorListener.accept(e);
      return null;
    }
    listener.accept(result);
    return result;
  }

  /** Approximates the size of an operation on the wire. */
  static long estimate(BulkOp op) {
    long size = ACTION_OVERHEAD + op.index().length() + (op.id() == null ? 0 : op.id().length());
    if (op.source() != null) {
      size += Json.canonical(op.source()).getBytes(StandardCharsets.UTF_8).length;
    }
    return size;
  }

  /** Builder for {@link BulkBuffer}. */
  public static final class Builder {

    private SearchEngine engine;
    private int maxActions = 500;
    private long maxBytes = 5L * 1024 * 1024;
    private Duration flushInterval;
    private Consumer<BulkResult> listener = result -> {
      if (result.errors()) {
        LOG.warn("Bulk request had {} failed items", result.failures().size());
      }
    };
    private Consumer<Throwable> errorListener = error -> LOG.error("Bulk request failed", error);

    private Builder() {}

    /** Sets the engine batches are sent to; required. */
    public Builder engine(SearchEngine value) {
      this.engine = value;
      return this;
    }

    /** Sends a batch once it holds this many operations; default 500. */
    public Builder maxActions(int value) {
      if (value < 1) {
        throw new IllegalArgumentException("maxActions must be at least 1: " + value);
      }
      this.maxActions = value;
      return this;
    }

    /**
     * Sends a batch once its approximate size, the canonical JSON of the sources plus a small
     * per-operation overhead, reaches this many bytes; default 5 MiB.
     */
    public Builder maxBytes(long value) {
      if (value < 1) {
        throw new IllegalArgumentException("maxBytes must be at least 1: " + value);
      }
      this.maxBytes = value;
      return this;
    }

    /**
     * Also sends waiting operations at this interval, from a daemon thread; null or zero (the
     * default) disables the timer.
     */
    public Builder flushInterval(Duration value) {
      this.flushInterval = value;
      return this;
    }

    /** Receives every batch result; by default failed items are counted in a warning. */
    public Builder listener(Consumer<BulkResult> value) {
      this.listener = Objects.requireNonNull(value, "listener");
      return this;
    }

    /** Receives exceptions thrown by the engine for a batch; by default they are logged. */
    public Builder errorListener(Consumer<Throwable> value) {
      this.errorListener = Objects.requireNonNull(value, "errorListener");
      return this;
    }

    /** Builds the buffer and starts its flush timer, if any. */
    public BulkBuffer build() {
      return new BulkBuffer(this);
    }
  }
}
