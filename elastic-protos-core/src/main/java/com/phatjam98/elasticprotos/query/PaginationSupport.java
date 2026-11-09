package com.phatjam98.elasticprotos.query;

import com.phatjam98.elasticprotos.engine.SearchResult;
import com.phatjam98.elasticprotos.proto.Pagination;
import com.phatjam98.elasticprotos.proto.PrimitiveValue;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Helpers to carry {@link Pagination} from one page of results to the next. */
public final class PaginationSupport {

  private PaginationSupport() {}

  /**
   * Returns the pagination that requests the page after {@code result}.
   *
   * <p>The cursor becomes the point-in-time id the engine returned (engines may rotate it), the
   * search_after values become the sort values of the last hit, and total is set from the
   * result. Size and keep_alive are kept. Without a cursor, {@code from} is advanced by the
   * number of hits instead, for shallow paging. An empty result keeps the previous search_after.
   *
   * @param previous the pagination of the request that produced {@code result}
   * @param result the page just received
   * @return the pagination of the next request
   */
  public static Pagination next(Pagination previous, SearchResult result) {
    Objects.requireNonNull(previous, "previous");
    Objects.requireNonNull(result, "result");
    Pagination.Builder next = previous.toBuilder();
    if (result.pitId() != null && !result.pitId().isEmpty()) {
      next.setCursor(result.pitId());
    }
    List<Object> lastSort = result.lastSort();
    if (lastSort != null && !lastSort.isEmpty()) {
      next.clearSearchAfter();
      for (Object value : lastSort) {
        next.addSearchAfter(toPrimitive(value));
      }
    }
    // -1 (SearchResult.NO_TOTAL) passes through: the engine did not count hits
    next.setTotal((int) Math.min(Integer.MAX_VALUE, Math.max(SearchResult.NO_TOTAL,
        result.total())));
    if (next.getCursor().isEmpty()) {
      next.setFrom(previous.getFrom() + result.hits().size());
    } else {
      next.setFrom(0);
    }
    return next.build();
  }

  /**
   * Returns the search_after values of a pagination as JSON values: integers as Long, other
   * numbers as Double, strings, booleans, and null for an unset value.
   */
  public static List<Object> searchAfter(Pagination pagination) {
    List<Object> values = new ArrayList<>();
    for (PrimitiveValue value : pagination.getSearchAfterList()) {
      switch (value.getKindCase()) {
        case INT_VALUE:
          values.add(value.getIntValue());
          break;
        case NUMBER_VALUE:
          values.add(value.getNumberValue());
          break;
        case STRING_VALUE:
          values.add(value.getStringValue());
          break;
        case BOOL_VALUE:
          values.add(value.getBoolValue());
          break;
        default:
          values.add(null);
          break;
      }
    }
    return values;
  }

  /**
   * Converts a sort value returned by the engine into a {@link PrimitiveValue}.
   *
   * @param value a JSON scalar or null
   * @return the value; unset for null
   */
  public static PrimitiveValue toPrimitive(Object value) {
    PrimitiveValue.Builder builder = PrimitiveValue.newBuilder();
    if (value == null) {
      return builder.build();
    }
    if (value instanceof Long || value instanceof Integer || value instanceof Short
        || value instanceof Byte) {
      return builder.setIntValue(((Number) value).longValue()).build();
    }
    if (value instanceof BigInteger big) {
      return big.bitLength() < 64
          ? builder.setIntValue(big.longValue()).build()
          : builder.setStringValue(big.toString()).build();
    }
    if (value instanceof Number number) {
      return builder.setNumberValue(number.doubleValue()).build();
    }
    if (value instanceof Boolean bool) {
      return builder.setBoolValue(bool).build();
    }
    return builder.setStringValue(value.toString()).build();
  }
}
