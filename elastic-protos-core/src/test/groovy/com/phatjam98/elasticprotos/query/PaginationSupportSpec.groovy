package com.phatjam98.elasticprotos.query

import static com.phatjam98.elasticprotos.query.Q.prim

import com.phatjam98.elasticprotos.engine.Hit
import com.phatjam98.elasticprotos.engine.SearchResult
import com.phatjam98.elasticprotos.proto.Pagination
import com.phatjam98.elasticprotos.proto.PrimitiveValue
import spock.lang.Specification

class PaginationSupportSpec extends Specification {

  static SearchResult result(String pitId, long total, List<List<Object>> sorts) {
    new SearchResult(total, true, sorts.withIndex().collect { sort, i ->
      new Hit('docs-v1', "d${i}", null, [:], sort)
    }, pitId, [:], 3)
  }

  def "next carries the rotated pit id, the last sort values and the total"() {
    given:
    def previous = Pagination.newBuilder().setSize(2).setCursor('pit-1').setKeepAlive('2m')
        .addSearchAfter(prim('old')).build()

    when:
    def next = PaginationSupport.next(previous,
        result('pit-2', 7, [[1L, 'a'], [5L, 'b', 2.5d, true, null]]))

    then:
    next.cursor == 'pit-2'
    next.keepAlive == '2m'
    next.size == 2
    next.total == 7
    next.from == 0
    PaginationSupport.searchAfter(next) == [5L, 'b', 2.5d, true, null]
  }

  def "next keeps the cursor and search_after when the engine returns neither"() {
    given:
    def previous = Pagination.newBuilder().setCursor('pit-1').addSearchAfter(prim(3L)).build()

    when:
    def next = PaginationSupport.next(previous, result(null, 3_000_000_000L, []))

    then:
    next.cursor == 'pit-1'
    PaginationSupport.searchAfter(next) == [3L]
    next.total == Integer.MAX_VALUE
  }

  def "without a cursor next advances from"() {
    expect:
    PaginationSupport.next(Pagination.newBuilder().setFrom(10).setSize(2).build(),
        result('', 20, [[1L], [2L]])).from == 12
  }

  def "sort values convert to primitive values"() {
    expect:
    PaginationSupport.toPrimitive(value) == expected

    where:
    value                        || expected
    null                         || PrimitiveValue.defaultInstance
    3                            || PrimitiveValue.newBuilder().setIntValue(3).build()
    (short) 4                    || PrimitiveValue.newBuilder().setIntValue(4).build()
    (byte) 5                     || PrimitiveValue.newBuilder().setIntValue(5).build()
    9L                           || PrimitiveValue.newBuilder().setIntValue(9).build()
    BigInteger.TEN               || PrimitiveValue.newBuilder().setIntValue(10).build()
    BigInteger.TWO.pow(70)       || PrimitiveValue.newBuilder().setStringValue('1180591620717411303424').build()
    1.5f                         || PrimitiveValue.newBuilder().setNumberValue(1.5).build()
    new BigDecimal('2.5')        || PrimitiveValue.newBuilder().setNumberValue(2.5).build()
    false                        || PrimitiveValue.newBuilder().setBoolValue(false).build()
    'x'                          || PrimitiveValue.newBuilder().setStringValue('x').build()
    new StringBuilder('y')       || PrimitiveValue.newBuilder().setStringValue('y').build()
  }
}
