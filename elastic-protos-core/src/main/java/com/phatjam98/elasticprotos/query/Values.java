package com.phatjam98.elasticprotos.query;

import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import com.phatjam98.elasticprotos.proto.DistanceType;
import com.phatjam98.elasticprotos.proto.FlatValue;
import com.phatjam98.elasticprotos.proto.GeoBoundingBoxValue;
import com.phatjam98.elasticprotos.proto.GeoDistanceValue;
import com.phatjam98.elasticprotos.proto.GeoShape;
import com.phatjam98.elasticprotos.proto.LatLon;
import com.phatjam98.elasticprotos.proto.PrimitiveList;
import com.phatjam98.elasticprotos.proto.PrimitiveValue;
import com.phatjam98.elasticprotos.proto.RangeValue;
import java.math.BigInteger;
import java.time.Instant;

/**
 * Static factories for {@link FlatValue}, the operand of a {@code FieldCondition}.
 *
 * <p>These are a shorthand over the generated builders, which remain the wire format:
 *
 * <pre>{@code
 * FlatValue active = Values.of("ACTIVE");
 * FlatValue size = Values.between(10_000, 100_000);
 * FlatValue near = Values.distance(36.17, -115.14, "50km");
 * }</pre>
 *
 * @see Criteria
 */
public final class Values {

  private Values() {}

  /**
   * Returns a string value.
   *
   * @param value the string
   * @return {@code string_value}
   */
  public static FlatValue of(String value) {
    return FlatValue.newBuilder().setStringValue(value).build();
  }

  /**
   * Returns an exact 64-bit integer value.
   *
   * @param value the integer
   * @return {@code int_value}
   */
  public static FlatValue of(long value) {
    return FlatValue.newBuilder().setIntValue(value).build();
  }

  /**
   * Returns a floating-point value.
   *
   * @param value the number
   * @return {@code number_value}
   */
  public static FlatValue of(double value) {
    return FlatValue.newBuilder().setNumberValue(value).build();
  }

  /**
   * Returns a boolean value.
   *
   * @param value the boolean
   * @return {@code bool_value}
   */
  public static FlatValue of(boolean value) {
    return FlatValue.newBuilder().setBoolValue(value).build();
  }

  /**
   * Returns a timestamp value.
   *
   * @param value the timestamp
   * @return {@code timestamp_value}
   */
  public static FlatValue of(Timestamp value) {
    return FlatValue.newBuilder().setTimestampValue(value).build();
  }

  /**
   * Returns a timestamp value.
   *
   * @param value the instant
   * @return {@code timestamp_value} with the instant's seconds and nanoseconds
   */
  public static FlatValue of(Instant value) {
    return of(Timestamp.newBuilder().setSeconds(value.getEpochSecond())
        .setNanos(value.getNano()).build());
  }

  /**
   * Returns the null value; {@code EQUALS} null matches documents without the field.
   *
   * @return {@code null_value}
   */
  public static FlatValue nullValue() {
    return FlatValue.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
  }

  /**
   * Returns a numeric range for {@code NUM_RANGE}. Date fields take epoch milliseconds.
   *
   * @param min the lower bound
   * @param minInclusive whether the lower bound matches
   * @param max the upper bound
   * @param maxInclusive whether the upper bound matches
   * @return {@code range_value}
   */
  public static FlatValue range(double min, boolean minInclusive, double max,
      boolean maxInclusive) {
    return FlatValue.newBuilder().setRangeValue(RangeValue.newBuilder()
        .setMin(min).setMinInclusive(minInclusive)
        .setMax(max).setMaxInclusive(maxInclusive)).build();
  }

  /**
   * Returns an inclusive numeric range for {@code NUM_RANGE}.
   *
   * @param min the inclusive lower bound
   * @param max the inclusive upper bound
   * @return {@code range_value}
   */
  public static FlatValue between(double min, double max) {
    return range(min, true, max, true);
  }

  /**
   * Returns an inclusive date range for {@code NUM_RANGE} in epoch milliseconds.
   *
   * @param from the inclusive start
   * @param to the inclusive end
   * @return {@code range_value}
   */
  public static FlatValue between(Instant from, Instant to) {
    return between(from.toEpochMilli(), to.toEpochMilli());
  }

  /**
   * Returns a range matching values at or above a bound for {@code NUM_RANGE}.
   *
   * @param value the inclusive lower bound (epoch milliseconds on date fields)
   * @return {@code range_value} with only a lower bound
   */
  public static FlatValue atLeast(double value) {
    return FlatValue.newBuilder().setRangeValue(RangeValue.newBuilder()
        .setMin(value).setMinInclusive(true)).build();
  }

  /**
   * Returns a range matching values strictly above a bound for {@code NUM_RANGE}.
   *
   * @param value the exclusive lower bound (epoch milliseconds on date fields)
   * @return {@code range_value} with only a lower bound
   */
  public static FlatValue greaterThan(double value) {
    return FlatValue.newBuilder().setRangeValue(RangeValue.newBuilder()
        .setMin(value).setMinInclusive(false)).build();
  }

  /**
   * Returns a range matching values at or below a bound for {@code NUM_RANGE}.
   *
   * @param value the inclusive upper bound (epoch milliseconds on date fields)
   * @return {@code range_value} with only an upper bound
   */
  public static FlatValue atMost(double value) {
    return FlatValue.newBuilder().setRangeValue(RangeValue.newBuilder()
        .setMax(value).setMaxInclusive(true)).build();
  }

  /**
   * Returns a range matching values strictly below a bound for {@code NUM_RANGE}.
   *
   * @param value the exclusive upper bound (epoch milliseconds on date fields)
   * @return {@code range_value} with only an upper bound
   */
  public static FlatValue lessThan(double value) {
    return FlatValue.newBuilder().setRangeValue(RangeValue.newBuilder()
        .setMax(value).setMaxInclusive(false)).build();
  }

  /**
   * Returns a range matching dates at or after an instant for {@code NUM_RANGE}.
   *
   * @param value the inclusive start
   * @return {@code range_value} with only a lower bound
   */
  public static FlatValue onOrAfter(Instant value) {
    return FlatValue.newBuilder().setRangeValue(RangeValue.newBuilder()
        .setMin(value.toEpochMilli()).setMinInclusive(true)).build();
  }

  /**
   * Returns a range matching dates strictly after an instant for {@code NUM_RANGE}.
   *
   * @param value the exclusive start
   * @return {@code range_value} with only a lower bound
   */
  public static FlatValue after(Instant value) {
    return FlatValue.newBuilder().setRangeValue(RangeValue.newBuilder()
        .setMin(value.toEpochMilli()).setMinInclusive(false)).build();
  }

  /**
   * Returns a range matching dates at or before an instant for {@code NUM_RANGE}.
   *
   * @param value the inclusive end
   * @return {@code range_value} with only an upper bound
   */
  public static FlatValue onOrBefore(Instant value) {
    return FlatValue.newBuilder().setRangeValue(RangeValue.newBuilder()
        .setMax(value.toEpochMilli()).setMaxInclusive(true)).build();
  }

  /**
   * Returns a range matching dates strictly before an instant for {@code NUM_RANGE}.
   *
   * @param value the exclusive end
   * @return {@code range_value} with only an upper bound
   */
  public static FlatValue before(Instant value) {
    return FlatValue.newBuilder().setRangeValue(RangeValue.newBuilder()
        .setMax(value.toEpochMilli()).setMaxInclusive(false)).build();
  }

  /**
   * Returns a list of scalars for {@code ANY_IN}.
   *
   * <p>Strings become {@code string_value}, booleans {@code bool_value}, {@code Long}, {@code
   * Integer}, {@code Short}, {@code Byte} and {@code BigInteger} {@code int_value}, other numbers
   * {@code number_value}, and enum constants the string of their name.
   *
   * @param values the items
   * @return {@code list_value}
   * @throws IllegalArgumentException when an item is null or of another type
   */
  public static FlatValue list(Object... values) {
    PrimitiveList.Builder list = PrimitiveList.newBuilder();
    for (Object value : values) {
      list.addValues(primitive(value));
    }
    return FlatValue.newBuilder().setListValue(list).build();
  }

  /**
   * Returns a point for the geo-shape operations.
   *
   * @param lat latitude in degrees
   * @param lon longitude in degrees
   * @return {@code lat_lon_value}
   */
  public static FlatValue latLon(double lat, double lon) {
    return FlatValue.newBuilder().setLatLonValue(point(lat, lon)).build();
  }

  /**
   * Returns an arc distance around a point for {@code GEO_DISTANCE}.
   *
   * @param lat latitude of the centre
   * @param lon longitude of the centre
   * @param distance the radius with its unit, such as {@code "10km"}
   * @return {@code geo_distance_value}
   */
  public static FlatValue distance(double lat, double lon, String distance) {
    return distance(lat, lon, distance, DistanceType.ARC);
  }

  /**
   * Returns a distance around a point for {@code GEO_DISTANCE}.
   *
   * @param lat latitude of the centre
   * @param lon longitude of the centre
   * @param distance the radius with its unit, such as {@code "10km"}
   * @param type how the distance is computed
   * @return {@code geo_distance_value}
   */
  public static FlatValue distance(double lat, double lon, String distance, DistanceType type) {
    return FlatValue.newBuilder().setGeoDistanceValue(GeoDistanceValue.newBuilder()
        .setPoint(point(lat, lon)).setDistance(distance).setDistanceType(type)).build();
  }

  /**
   * Returns a rectangle given by its corners for {@code GEO_BOUNDING_BOX}.
   *
   * @param topLat latitude of the top edge
   * @param leftLon longitude of the left edge
   * @param bottomLat latitude of the bottom edge
   * @param rightLon longitude of the right edge
   * @return {@code geo_bounding_box_value} with {@code top_left} and {@code bottom_right}
   */
  public static FlatValue boundingBox(double topLat, double leftLon, double bottomLat,
      double rightLon) {
    return FlatValue.newBuilder().setGeoBoundingBoxValue(GeoBoundingBoxValue.newBuilder()
        .setTopLeft(point(topLat, leftLon)).setBottomRight(point(bottomLat, rightLon))).build();
  }

  /**
   * Returns a rectangle given as WKT for {@code GEO_BOUNDING_BOX}.
   *
   * @param wkt a WKT {@code BBOX (minLon, maxLon, maxLat, minLat)}
   * @return {@code geo_bounding_box_value} with {@code wkt}
   */
  public static FlatValue boundingBox(String wkt) {
    return FlatValue.newBuilder()
        .setGeoBoundingBoxValue(GeoBoundingBoxValue.newBuilder().setWkt(wkt)).build();
  }

  /**
   * Returns a shape given as WKT for the geo-shape operations.
   *
   * @param wkt the geometry, such as {@code "POINT (-115.14 36.17)"}
   * @return {@code geo_shape_value} with {@code wkt}
   */
  public static FlatValue wkt(String wkt) {
    return FlatValue.newBuilder().setGeoShapeValue(GeoShape.newBuilder().setWkt(wkt)).build();
  }

  /**
   * Returns a shape given as GeoJSON for the geo-shape operations.
   *
   * @param geoJson the geometry object as JSON text
   * @return {@code geo_shape_value} with {@code geojson}
   */
  public static FlatValue geoJson(String geoJson) {
    return FlatValue.newBuilder().setGeoShapeValue(GeoShape.newBuilder().setGeojson(geoJson))
        .build();
  }

  /**
   * Converts a plain Java value to a {@link FlatValue}: null is {@link #nullValue()}, a {@code
   * FlatValue} is returned as is, a {@link Timestamp} or {@link Instant} is a timestamp, and
   * scalars follow {@link #list}.
   */
  static FlatValue from(Object value) {
    if (value == null) {
      return nullValue();
    }
    if (value instanceof FlatValue) {
      return (FlatValue) value;
    }
    if (value instanceof Timestamp) {
      return of((Timestamp) value);
    }
    if (value instanceof Instant) {
      return of((Instant) value);
    }
    return ValueConversions.of(primitive(value));
  }

  /** Converts a string, number, boolean or enum constant to a {@link PrimitiveValue}. */
  static PrimitiveValue primitive(Object value) {
    PrimitiveValue.Builder builder = PrimitiveValue.newBuilder();
    if (value instanceof String) {
      return builder.setStringValue((String) value).build();
    }
    if (value instanceof Boolean) {
      return builder.setBoolValue((Boolean) value).build();
    }
    if (value instanceof Long || value instanceof Integer || value instanceof Short
        || value instanceof Byte) {
      return builder.setIntValue(((Number) value).longValue()).build();
    }
    if (value instanceof BigInteger) {
      return builder.setIntValue(((BigInteger) value).longValueExact()).build();
    }
    if (value instanceof Number) {
      return builder.setNumberValue(((Number) value).doubleValue()).build();
    }
    if (value instanceof Enum) {
      return builder.setStringValue(((Enum<?>) value).name()).build();
    }
    throw new IllegalArgumentException("Expected a string, number, boolean or enum but got "
        + (value == null ? "null" : value.getClass().getName() + " " + value));
  }

  private static LatLon point(double lat, double lon) {
    return LatLon.newBuilder().setLat(lat).setLon(lon).build();
  }
}
