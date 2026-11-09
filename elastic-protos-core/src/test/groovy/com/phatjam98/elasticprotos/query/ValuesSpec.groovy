package com.phatjam98.elasticprotos.query

import static com.phatjam98.elasticprotos.query.Q.*

import com.google.protobuf.Timestamp
import com.phatjam98.elasticprotos.proto.DistanceType
import com.phatjam98.elasticprotos.proto.FlatValue
import com.phatjam98.elasticprotos.proto.GeoBoundingBoxValue
import com.phatjam98.elasticprotos.proto.GeoDistanceValue
import com.phatjam98.elasticprotos.proto.GeoShape
import com.phatjam98.elasticprotos.proto.PrimitiveList
import com.phatjam98.elasticprotos.proto.PrimitiveValue
import com.phatjam98.elasticprotos.proto.example.DocumentType
import java.time.Instant
import spock.lang.Specification

class ValuesSpec extends Specification {

  def "scalar factories match the hand-built values: #description"() {
    expect:
    actual == expected

    where:
    description           | actual                                                    | expected
    'string'              | Values.of('ACTIVE')                                       | str('ACTIVE')
    'long'                | Values.of(42L)                                            | integer(42)
    'int widens to long'  | Values.of(42)                                             | integer(42)
    'double'              | Values.of(1.5d)                                           | num(1.5)
    'boolean'             | Values.of(true)                                           | bool(true)
    'timestamp'           | Values.of(Timestamp.newBuilder().setSeconds(10).setNanos(5).build()) | ts(10, 5)
    'instant'             | Values.of(Instant.ofEpochSecond(1_700_000_000L, 123))     | ts(1_700_000_000L, 123)
    'null'                | Values.nullValue()                                        | nul()
    'range'               | Values.range(1, false, 9, true)                           | range(1, 9, false, true)
    'between'             | Values.between(1, 9)                                      | range(1, 9)
    'between instants'    | Values.between(Instant.ofEpochMilli(1000), Instant.ofEpochMilli(2000)) | range(1000, 2000)
  }

  def "open-ended range factories set exactly one bound: #description"() {
    expect:
    value.rangeValue.hasMin() == hasMin
    value.rangeValue.hasMax() == hasMax
    value.rangeValue.min == min
    value.rangeValue.max == max
    value.rangeValue.minInclusive == minInc
    value.rangeValue.maxInclusive == maxInc

    where:
    description  | value                                         | hasMin | hasMax | min  | max  | minInc | maxInc
    'atLeast'    | Values.atLeast(5)                             | true   | false  | 5d   | 0d   | true   | false
    'greaterThan'| Values.greaterThan(5)                         | true   | false  | 5d   | 0d   | false  | false
    'atMost'     | Values.atMost(7)                              | false  | true   | 0d   | 7d   | false  | true
    'lessThan'   | Values.lessThan(7)                            | false  | true   | 0d   | 7d   | false  | false
    'onOrAfter'  | Values.onOrAfter(Instant.ofEpochMilli(1000))  | true   | false  | 1000d| 0d   | true   | false
    'after'      | Values.after(Instant.ofEpochMilli(1000))      | true   | false  | 1000d| 0d   | false  | false
    'onOrBefore' | Values.onOrBefore(Instant.ofEpochMilli(2000)) | false  | true   | 0d   | 2000d| false  | true
    'before'     | Values.before(Instant.ofEpochMilli(2000))     | false  | true   | 0d   | 2000d| false  | false
  }

  def "geo factories match the hand-built values: #description"() {
    expect:
    actual == expected

    where:
    description           | actual                                         | expected
    'lat/lon'             | Values.latLon(36.1, -115.1)                    | FlatValue.newBuilder().setLatLonValue(ll(36.1, -115.1)).build()
    'arc distance'        | Values.distance(36.1, -115.1, '10km')          | FlatValue.newBuilder().setGeoDistanceValue(GeoDistanceValue.newBuilder().setPoint(ll(36.1, -115.1)).setDistance('10km')).build()
    'plane distance'      | Values.distance(36.1, -115.1, '5mi', DistanceType.PLANE) | FlatValue.newBuilder().setGeoDistanceValue(GeoDistanceValue.newBuilder().setPoint(ll(36.1, -115.1)).setDistance('5mi').setDistanceType(DistanceType.PLANE)).build()
    'box by corners'      | Values.boundingBox(38, -120, 34, -104)         | FlatValue.newBuilder().setGeoBoundingBoxValue(GeoBoundingBoxValue.newBuilder().setTopLeft(ll(38, -120)).setBottomRight(ll(34, -104))).build()
    'box by wkt'          | Values.boundingBox('BBOX (-120, -104, 38, 34)') | FlatValue.newBuilder().setGeoBoundingBoxValue(GeoBoundingBoxValue.newBuilder().setWkt('BBOX (-120, -104, 38, 34)')).build()
    'wkt shape'           | Values.wkt('POINT (1 2)')                      | FlatValue.newBuilder().setGeoShapeValue(GeoShape.newBuilder().setWkt('POINT (1 2)')).build()
    'geojson shape'       | Values.geoJson('{"type":"Point"}')            | FlatValue.newBuilder().setGeoShapeValue(GeoShape.newBuilder().setGeojson('{"type":"Point"}')).build()
  }

  def "list converts strings, numbers, booleans and enum constants"() {
    when:
    FlatValue value = Values.list('a', true, 1, 2L, (short) 3, (byte) 4, 5G, 1.5d, 2.5f, 3.25G,
        DocumentType.MOTION)

    then:
    value.listValue.valuesList == [
        PrimitiveValue.newBuilder().setStringValue('a').build(),
        PrimitiveValue.newBuilder().setBoolValue(true).build(),
        PrimitiveValue.newBuilder().setIntValue(1).build(),
        PrimitiveValue.newBuilder().setIntValue(2).build(),
        PrimitiveValue.newBuilder().setIntValue(3).build(),
        PrimitiveValue.newBuilder().setIntValue(4).build(),
        PrimitiveValue.newBuilder().setIntValue(5).build(),
        PrimitiveValue.newBuilder().setNumberValue(1.5).build(),
        PrimitiveValue.newBuilder().setNumberValue(2.5).build(),
        PrimitiveValue.newBuilder().setNumberValue(3.25).build(),
        PrimitiveValue.newBuilder().setStringValue('MOTION').build(),
    ]
    Values.list() == FlatValue.newBuilder().setListValue(PrimitiveList.defaultInstance).build()
    Values.list('x', 1L) == list('x', 1L)
  }

  def "list rejects #description"() {
    when:
    Values.list('ok', item)

    then:
    def e = thrown(IllegalArgumentException)
    e.message.contains(message)

    where:
    description   | item                       | message
    'null'        | null                       | 'got null'
    'a map'       | [a: 1]                     | 'java.util.LinkedHashMap'
    'a timestamp' | Timestamp.defaultInstance  | 'com.google.protobuf.Timestamp'
  }

  def "from converts any supported Java value"() {
    expect:
    Values.from(null) == nul()
    Values.from(Timestamp.newBuilder().setSeconds(7).build()) == ts(7)
    Values.from(Instant.ofEpochSecond(7)) == ts(7)
    Values.from('s') == str('s')
    Values.from(3) == integer(3)
    Values.from(3.5d) == num(3.5)
    Values.from(false) == bool(false)
    Values.from(DocumentType.ORDER) == str('ORDER')
  }

  def "from returns a FlatValue unchanged"() {
    given:
    FlatValue value = Values.latLon(1, 2)

    expect:
    Values.from(value).is(value)
  }

  def "Instant helpers reject instants beyond epoch milliseconds: #description"() {
    when:
    call()

    then:
    def e = thrown(IllegalArgumentException)
    e.message.contains('epoch milliseconds')

    where:
    description | call
    'after'     | { Values.after(Instant.MAX) }
    'before'    | { Values.before(Instant.MIN) }
    'between'   | { Values.between(Instant.MIN, Instant.EPOCH) }
  }

  def "Instant helpers truncate sub-millisecond precision"() {
    expect:
    Values.atLeast(1.0d) != null
    Values.onOrAfter(Instant.ofEpochSecond(1, 999_999)).rangeValue.min == 1000d
  }
}
