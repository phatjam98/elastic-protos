package com.phatjam98.elasticprotos.query;

import java.math.BigInteger;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * The value of {@code track_total_hits} in a compiled search body: count every hit ({@link
 * #exact()}, sent as {@code true}), count none ({@link #none()}, sent as {@code false}), count up
 * to a bound ({@link #upTo(int)}, sent as the number), or leave the key out so that the engine's
 * own default applies ({@link #engineDefault()}; Elasticsearch and OpenSearch count up to 10000).
 */
public final class TrackTotalHits {

  /** How hits are counted. */
  public enum Kind {
    /** Count every hit; sent as {@code true}. */
    EXACT,
    /** Count no hits; sent as {@code false}. */
    NONE,
    /** Count hits up to {@link TrackTotalHits#limit()}; sent as that number. */
    UP_TO,
    /** Leave {@code track_total_hits} out of the body; the engine's default applies. */
    DEFAULT
  }

  private static final TrackTotalHits EXACT = new TrackTotalHits(Kind.EXACT, 0);
  private static final TrackTotalHits NONE = new TrackTotalHits(Kind.NONE, 0);
  private static final TrackTotalHits DEFAULT = new TrackTotalHits(Kind.DEFAULT, 0);
  private static final Pattern INTEGER = Pattern.compile("-?[0-9]+");
  private static final BigInteger MAX = BigInteger.valueOf(Integer.MAX_VALUE);

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

  /** Leaves {@code track_total_hits} out of the body, so that the engine's default applies. */
  public static TrackTotalHits engineDefault() {
    return DEFAULT;
  }

  /**
   * Counts hits up to {@code limit}; beyond it the total is a lower bound.
   *
   * @param limit the bound, at least 0
   * @return the setting
   * @throws IllegalArgumentException when the bound is negative
   */
  public static TrackTotalHits upTo(int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException("trackTotalHits limit must not be negative: " + limit);
    }
    return new TrackTotalHits(Kind.UP_TO, limit);
  }

  /** Returns {@link #exact()} for true and {@link #none()} for false. */
  public static TrackTotalHits of(boolean track) {
    return track ? EXACT : NONE;
  }

  /**
   * Parses a configuration value: {@code true}, {@code false} or {@code default} (any case), or a
   * decimal integer between 0 and {@link Integer#MAX_VALUE} without a sign; surrounding blanks are
   * ignored.
   *
   * @param value the text
   * @return the setting
   * @throws NullPointerException when the text is null
   * @throws IllegalArgumentException when the text is none of these, or the number is negative or
   *     out of range
   */
  public static TrackTotalHits parse(CharSequence value) {
    Objects.requireNonNull(value, "value");
    String text = value.toString().trim().toLowerCase(Locale.ROOT);
    if (text.equals("true")) {
      return EXACT;
    }
    if (text.equals("false")) {
      return NONE;
    }
    if (text.equals("default")) {
      return DEFAULT;
    }
    if (!INTEGER.matcher(text).matches()) {
      throw new IllegalArgumentException(
          "trackTotalHits must be true, false, default or a number, got " + value);
    }
    BigInteger number = new BigInteger(text);
    if (number.signum() < 0) {
      throw new IllegalArgumentException("trackTotalHits limit must not be negative: " + text);
    }
    if (number.compareTo(MAX) > 0) {
      throw new IllegalArgumentException("trackTotalHits limit out of range (0 to "
          + Integer.MAX_VALUE + "): " + text);
    }
    return upTo(number.intValue());
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
   * Boolean#FALSE}, an {@link Integer}, or null for {@link #engineDefault()}, whose key is left
   * out.
   */
  public Object toJson() {
    return switch (kind) {
      case EXACT -> Boolean.TRUE;
      case NONE -> Boolean.FALSE;
      case UP_TO -> limit;
      case DEFAULT -> null;
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

  /**
   * Returns {@code true}, {@code false}, the bound, or {@code default} for {@link
   * #engineDefault()}.
   */
  @Override
  public String toString() {
    return kind == Kind.DEFAULT ? "default" : String.valueOf(toJson());
  }
}
