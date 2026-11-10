package com.phatjam98.elasticprotos.testkit.soak;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The migration phases a soak run has seen, with the time each was reported, and the phase the
 * run is in now. Writers and readers label their observations with {@link #current()}.
 */
public final class PhaseClock {

  /** The label before the migration starts. */
  public static final String BEFORE = "BEFORE";
  /** The label while the migration runs but has reported no phase yet. */
  public static final String STARTED = "STARTED";
  /** The label after the migration returned. */
  public static final String AFTER = "AFTER";

  /**
   * A reported phase.
   *
   * @param phase the phase name
   * @param at epoch millis
   * @param source who reported it ({@code parent}, {@code child-<n>}, ...)
   */
  public record Event(String phase, long at, String source) {}

  private final List<Event> events = new ArrayList<>();
  private volatile String current = BEFORE;

  /** Records a phase. */
  public synchronized void mark(String phase, long at, String source) {
    events.add(new Event(phase, at, source));
    current = phase;
  }

  /** Records a phase reported now by the parent. */
  public void mark(String phase) {
    mark(phase, System.currentTimeMillis(), "parent");
  }

  /** Returns the current phase label. */
  public String current() {
    return current;
  }

  /** Returns the events so far. */
  public synchronized List<Event> events() {
    return List.copyOf(events);
  }

  /** Returns the time of the last event with this phase, or -1. */
  public synchronized long last(String phase) {
    for (int i = events.size() - 1; i >= 0; i--) {
      if (events.get(i).phase().equals(phase)) {
        return events.get(i).at();
      }
    }
    return -1;
  }

  /** Returns the time of the first event with this phase, or -1. */
  public synchronized long first(String phase) {
    for (Event event : events) {
      if (event.phase().equals(phase)) {
        return event.at();
      }
    }
    return -1;
  }

  /** Returns the time of the last event with this phase at or before {@code limit}, or -1. */
  public synchronized long lastBefore(String phase, long limit) {
    for (int i = events.size() - 1; i >= 0; i--) {
      Event event = events.get(i);
      if (event.phase().equals(phase) && event.at() <= limit) {
        return event.at();
      }
    }
    return -1;
  }

  /** Returns whether a phase has been reported. */
  public synchronized boolean seen(String phase) {
    return last(phase) >= 0;
  }

  /** Returns the events as JSON-shaped maps, times relative to {@code origin}. */
  public synchronized List<Object> toList(long origin) {
    List<Object> list = new ArrayList<>();
    for (Event event : events) {
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("phase", event.phase());
      map.put("atMs", event.at() - origin);
      map.put("source", event.source());
      list.add(map);
    }
    return list;
  }
}
