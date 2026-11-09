package com.phatjam98.elasticprotos.query;

import java.util.Locale;
import java.util.Objects;

/**
 * The value of {@code track_total_hits} in a compiled search body: count every hit ({@link
 * #exact()}, sent as {@code true}), count none ({@link #none()}, sent as {@code false}), or count
 * up to a bound ({@link #upTo(long)}, sent as the number).
 */
public final class TrackTotalHits {

  /** How hits are counted. */
  public enum Kind {
    /** Count every hit; sent as {@code true}. */
    EXACT,
    /** Count no hits; sent as {@code false}. */
    NONE,
    /** Count hits up to {@link TrackTotalHits#limit()}; sent as that number. */
    UP_TO
  }

  private static final TrackTotalHits EXACT = new TrackTotalHits(Kind.EXACT, 0);
  private static final TrackTotalHits NONE = new TrackTotalHits(Kind.NONE, 0);

  private final Kind kind;
  private final int limit;

  private TrackTotalHits(Kind kind, int limit) {
    this.kind = kind;
    this.limit = limit;
  }

  /** Counts every hit; {@code track_total_hits: true}. */
  public static TrackTotalHits exact() {
    return EXACT;
  }

  /** Counts no hits; {@code track_total_hits: false}. */
  public static TrackTotalHits none() {
    return NONE;
  }

  /**
   * Counts hits up to {@code limit}; beyond it the total is a lower bound.
   *
   * @param limit the bound, between 0 and {@link Integer#MAX_VALUE} as the engines accept
   * @return the setting
   * @throws IllegalArgumentException when the bound is negative or above {@link
   *     Integer#MAX_VALUE}
   */
  public static TrackTotalHits upTo(long limit) {
    if (limit < 0 || limit > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "trackTotalHits limit must be between 0 and " + Integer.MAX_VALUE + ", got " + limit);
    }
    return new TrackTotalHits(Kind.UP_TO, (int) limit);
  }

  /** Returns {@link #exact()} for true and {@link #none()} for false. */
  public static TrackTotalHits of(boolean track) {
    return track ? EXACT : NONE;
  }

  /**
   * Parses a configuration value: {@code true}, {@code false} (any case, surrounding blanks
   * ignored) or a non-negative integer.
   *
   * @param value the text
   * @return the setting
   * @throws IllegalArgumentException when the text is none of these
   */
  public static TrackTotalHits parse(String value) {
    Objects.requireNonNull(value, "value");
    String text = value.trim().toLowerCase(Locale.ROOT);
    if (text.equals("true")) {
      return EXACT;
    }
    if (text.equals("false")) {
      return NONE;
    }
    long limit;
    try {
      limit = Long.parseLong(text);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "trackTotalHits must be true, false or a number, got " + value, e);
    }
    return upTo(limit);
  }

  /** Returns how hits are counted. */
  public Kind kind() {
    return kind;
  }

  /** Returns the bound of {@link Kind#UP_TO}, or 0 for the other kinds. */
  public int limit() {
    return limit;
  }

  /**
   * Returns the value as it goes into the search body: {@link Boolean#TRUE}, {@link
   * Boolean#FALSE} or an {@link Integer}.
   */
  public Object toJson() {
    return switch (kind) {
      case EXACT -> Boolean.TRUE;
      case NONE -> Boolean.FALSE;
      case UP_TO -> limit;
    };
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof TrackTotalHits that && kind == that.kind && limit == that.limit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(kind, limit);
  }

  /** Returns the body value as text: {@code true}, {@code false} or the bound. */
  @Override
  public String toString() {
    return String.valueOf(toJson());
  }
}
