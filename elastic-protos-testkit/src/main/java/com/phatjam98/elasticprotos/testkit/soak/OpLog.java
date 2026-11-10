package com.phatjam98.elasticprotos.testkit.soak;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The authoritative operation log of a soak run: for every document id, the writes the engine
 * acknowledged, in the order they were acknowledged.
 *
 * <p>Every id is owned by exactly one writer thread, which issues its operations one at a time, so
 * the acknowledged order of an id is its order. A write enters the log only once it was
 * acknowledged; a write whose outcome is unknown (timeout, I/O error) makes the id uncertain, and
 * an uncertain id is left out of the comparison. Readers take consistent views of an id through
 * {@link #view}.
 */
public final class OpLog {

  /** The kinds of acknowledged writes. */
  public enum OpType {
    /** Bulk-indexed before the run. */
    SEED,
    /** A full write of an id the log holds as absent. */
    CREATE,
    /** A full write of an id the log holds as present. */
    INDEX,
    /** A partial update ({@code _update} with {@code doc}). */
    UPDATE,
    /** A delete. */
    DELETE
  }

  /**
   * One acknowledged write.
   *
   * @param type what was written
   * @param body the full document (seed, create, index), the partial document (update), or null
   *     (delete)
   * @param startedAt when the request was sent, epoch millis
   * @param ackedAt when the response arrived, epoch millis
   * @param index the concrete index the engine reported, or null when unknown
   * @param found for a delete, whether the engine found the document; for a create or index,
   *     whether the engine reported {@code updated} (the document existed); unused otherwise
   */
  public record Entry(OpType type, Map<String, Object> body, long startedAt, long ackedAt,
      String index, boolean found) {

    /** Copies the body. */
    public Entry {
      body = body == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(body));
    }
  }

  /** The writes of one id. */
  public static final class History {
    private final List<Entry> entries = new ArrayList<>();
    private boolean uncertain;
    private boolean inFlight;
    private long changes;

    /** Returns a copy of the acknowledged writes, in order. */
    public synchronized List<Entry> entries() {
      return List.copyOf(entries);
    }

    /** Returns whether a write of unknown outcome makes the expected state unknown. */
    public synchronized boolean uncertain() {
      return uncertain;
    }

    synchronized void begin() {
      inFlight = true;
      changes++;
    }

    synchronized void ack(Entry entry) {
      entries.add(entry);
      inFlight = false;
      changes++;
    }

    synchronized void rejected() {
      inFlight = false;
      changes++;
    }

    synchronized void unknown() {
      uncertain = true;
      inFlight = false;
      changes++;
    }
  }

  /**
   * A consistent view of an id for a reader.
   *
   * @param changes a counter that moves whenever a write starts or ends
   * @param expected the expected document, or null when absent
   * @param lastIndex the concrete index of the last acknowledged write, or null
   * @param entries the acknowledged writes at the time of the view
   */
  public record View(long changes, Map<String, Object> expected, String lastIndex,
      List<Entry> entries) {}

  private final Map<String, History> byId = new ConcurrentHashMap<>();

  /** Returns the history of an id, creating it. */
  public History history(String id) {
    return byId.computeIfAbsent(id, key -> new History());
  }

  /** Records a write that is about to be sent. */
  public void begin(String id) {
    history(id).begin();
  }

  /** Records an acknowledged write. */
  public void ack(String id, Entry entry) {
    history(id).ack(entry);
  }

  /** Records a write the engine rejected: it did not change the document. */
  public void rejected(String id) {
    history(id).rejected();
  }

  /** Records a write whose outcome is unknown: the id is no longer compared. */
  public void unknown(String id) {
    history(id).unknown();
  }

  /** Returns the ids in the log. */
  public Set<String> ids() {
    return byId.keySet();
  }

  /** Returns the histories by id, sorted by id. */
  public Map<String, List<Entry>> snapshot() {
    Map<String, List<Entry>> copy = new TreeMap<>();
    byId.forEach((id, history) -> copy.put(id, history.entries()));
    return copy;
  }

  /** Returns the ids whose outcome is unknown. */
  public Set<String> uncertainIds() {
    Set<String> ids = new java.util.TreeSet<>();
    byId.forEach((id, history) -> {
      if (history.uncertain()) {
        ids.add(id);
      }
    });
    return ids;
  }

  /**
   * Returns a view of an id when no write of it is in flight and its outcome is known, or null.
   */
  public View view(String id) {
    History history = byId.get(id);
    if (history == null) {
      return null;
    }
    synchronized (history) {
      if (history.inFlight || history.uncertain) {
        return null;
      }
      String last = history.entries.isEmpty() ? null
          : history.entries.get(history.entries.size() - 1).index();
      return new View(history.changes, replay(history.entries), last,
          List.copyOf(history.entries));
    }
  }

  /** Returns whether an id changed since a view was taken, or is now in flight. */
  public boolean changedSince(String id, View view) {
    History history = byId.get(id);
    synchronized (history) {
      return history.inFlight || history.changes != view.changes();
    }
  }

  /**
   * Replays writes into the document they leave behind.
   *
   * @param entries the writes in order
   * @return the document, or null when absent
   */
  public static Map<String, Object> replay(List<Entry> entries) {
    return replay(null, entries);
  }

  /**
   * Replays writes onto a starting document.
   *
   * @param start the document before the first write, or null when absent
   * @param entries the writes in order
   * @return the document, or null when absent; a partial update of an absent document leaves it
   *     absent (the engine answers 404 and the write is never acknowledged)
   */
  public static Map<String, Object> replay(Map<String, Object> start, List<Entry> entries) {
    Map<String, Object> state = start == null ? null : new LinkedHashMap<>(start);
    for (Entry entry : entries) {
      switch (entry.type()) {
        case DELETE:
          state = null;
          break;
        case UPDATE:
          if (state != null) {
            state.putAll(entry.body());
          }
          break;
        default:
          state = new LinkedHashMap<>(entry.body());
          break;
      }
    }
    return state;
  }
}
