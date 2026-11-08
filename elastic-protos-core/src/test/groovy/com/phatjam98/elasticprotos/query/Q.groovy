package com.phatjam98.elasticprotos.query

import com.google.protobuf.NullValue
import com.google.protobuf.Timestamp
import com.phatjam98.elasticprotos.proto.FieldCondition
import com.phatjam98.elasticprotos.proto.FlatValue
import com.phatjam98.elasticprotos.proto.FullTextOptions
import com.phatjam98.elasticprotos.proto.LatLon
import com.phatjam98.elasticprotos.proto.Pagination
import com.phatjam98.elasticprotos.proto.PrimitiveList
import com.phatjam98.elasticprotos.proto.PrimitiveValue
import com.phatjam98.elasticprotos.proto.RangeValue
import com.phatjam98.elasticprotos.proto.SearchCondition
import com.phatjam98.elasticprotos.proto.SearchCriteria
import com.phatjam98.elasticprotos.proto.SearchOperationType
import com.phatjam98.elasticprotos.proto.SortingCriteria

/** Terse builders for query compiler tests. */
class Q {

  static FlatValue str(String s) { FlatValue.newBuilder().setStringValue(s).build() }

  static FlatValue num(double d) { FlatValue.newBuilder().setNumberValue(d).build() }

  static FlatValue integer(long l) { FlatValue.newBuilder().setIntValue(l).build() }

  static FlatValue bool(boolean b) { FlatValue.newBuilder().setBoolValue(b).build() }

  static FlatValue nul() { FlatValue.newBuilder().setNullValue(NullValue.NULL_VALUE).build() }

  static FlatValue ts(long seconds, int nanos = 0) {
    FlatValue.newBuilder().setTimestampValue(
        Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos)).build()
  }

  static FlatValue range(double min, double max, boolean minInc = true, boolean maxInc = true) {
    FlatValue.newBuilder().setRangeValue(RangeValue.newBuilder()
        .setMin(min).setMax(max).setMinInclusive(minInc).setMaxInclusive(maxInc)).build()
  }

  static PrimitiveValue prim(Object v) {
    def b = PrimitiveValue.newBuilder()
    switch (v) {
      case String: return b.setStringValue(v).build()
      case Boolean: return b.setBoolValue(v).build()
      case Long: case Integer: return b.setIntValue(v as long).build()
      case Number: return b.setNumberValue(v as double).build()
      default: return b.build()
    }
  }

  static FlatValue list(Object... values) {
    FlatValue.newBuilder().setListValue(
        PrimitiveList.newBuilder().addAllValues(values.collect { prim(it) })).build()
  }

  static LatLon ll(double lat, double lon) { LatLon.newBuilder().setLat(lat).setLon(lon).build() }

  static FieldCondition fc(String field, SearchOperationType op,
      FlatValue value = FlatValue.defaultInstance, boolean negate = false) {
    FieldCondition.newBuilder().setField(field).setOperation(op).setValue(value)
        .setNegate(negate).build()
  }

  static FieldCondition fullText(String query, FullTextOptions options = FullTextOptions.defaultInstance,
      String field = '') {
    FieldCondition.newBuilder().setField(field).setOperation(SearchOperationType.FULL_TEXT)
        .setValue(str(query)).setFullText(options).build()
  }

  static SearchCondition and(FieldCondition... conditions) {
    SearchCondition.newBuilder().addAllFieldCondition(conditions as List).build()
  }

  static SearchCriteria where(FieldCondition... conditions) {
    SearchCriteria.newBuilder().addSearchCondition(and(conditions)).build()
  }

  static SearchCriteria anyOf(SearchCondition... conditions) {
    SearchCriteria.newBuilder().addAllSearchCondition(conditions as List).build()
  }

  static SearchCriteria sorted(SortingCriteria... sorts) {
    SearchCriteria.newBuilder().addAllSortingCriteria(sorts as List).build()
  }

  static SearchCriteria paged(Pagination pagination, SearchCriteria base = SearchCriteria.defaultInstance) {
    base.toBuilder().setPagination(pagination).build()
  }
}
