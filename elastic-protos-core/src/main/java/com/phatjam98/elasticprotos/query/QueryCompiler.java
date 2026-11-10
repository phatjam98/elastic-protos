package com.phatjam98.elasticprotos.query;

import static com.phatjam98.elasticprotos.query.NestedPathResolver.object;

import com.phatjam98.elasticprotos.engine.rest.JsonRestEngine;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.proto.Pagination;
import com.phatjam98.elasticprotos.proto.SearchCondition;
import com.phatjam98.elasticprotos.proto.SearchCriteria;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Compiles {@link SearchCriteria} into engine search bodies, validated and typed against an
 * {@link IndexDefinition}.
 *
 * <p>The criteria are an OR of conditions, each an AND of field predicates. Every condition
 * becomes a {@code bool} query with non-scoring predicates in {@code filter}, scoring ones
 * ({@code FULL_TEXT}, and {@code TEXT_MATCHES}/{@code TEXT_CONTAINS} on text) in {@code must} and
 * negated ones in {@code must_not}; several conditions are combined with {@code bool.should} and
 * {@code minimum_should_match: 1}. Predicates on fields inside nested objects are wrapped by
 * {@link NestedPathResolver}.
 *
 * <p>{@code NUM_RANGE} emits only the bounds that are present ({@code RangeValue.min} and
 * {@code max} are proto3 {@code optional}), so a range may be open at either end; only the
 * natural-side infinity ({@code -Infinity} min, {@code +Infinity} max) counts as unset. A range
 * with no bound, a NaN bound, an empty range ({@code min > max}, equal bounds that are not both
 * inclusive, {@code +Infinity} min or {@code -Infinity} max) fails with a {@link
 * QueryCompileException} in strict and lenient mode alike.
 *
 * <p>Aggregations ({@code SearchCriteria.aggregation}) compile into {@code aggs}, checked against
 * the mapped type of each field: terms, date histograms, histograms, ranges, filter, filters,
 * composite and single-value metrics, with sub-aggregations down to {@link
 * QueryCompilerConfig#maxAggregationDepth()} levels. Aggregations on fields inside nested objects
 * are wrapped in {@code nested} aggregations named {@code <name>__nested} (then {@code
 * <name>__nested2}, ...) and, to step out of a nested bucket, a {@code reverse_nested} named
 * {@code <name>__reverse}; names therefore must not contain {@code __}. {@link
 * AggregationResults} unwraps them. Highlighting ({@code SearchCriteria.highlight}) compiles into
 * {@code highlight} over text fields outside nested objects.
 *
 * <p>Instances are immutable and thread-safe.
 */
public final class QueryCompiler {

  private static final String SHARD_DOC = "_shard_doc";

  private final QueryCompilerConfig config;

  private QueryCompiler(QueryCompilerConfig config) {
    this.config = Objects.requireNonNull(config, "config");
  }

  /** Returns a compiler with {@link QueryCompilerConfig#standard()}. */
  public static QueryCompiler withDefaults() {
    return new QueryCompiler(QueryCompilerConfig.standard());
  }

  /** Returns a compiler with the given settings. */
  public static QueryCompiler of(QueryCompilerConfig config) {
    return new QueryCompiler(config);
  }

  /** Returns the settings. */
  public QueryCompilerConfig config() {
    return config;
  }

  /**
   * Compiles a full search body: {@code query}, {@code sort}, paging ({@code from}/{@code size} or
   * {@code pit}/{@code search_after}), {@code track_total_hits}, and {@code aggs} and {@code
   * highlight} when the criteria ask for them (read aggregation results with {@link
   * AggregationResults}). A negative {@code pagination.size} compiles to {@code size: 0}, for
   * searches that only aggregate. A page larger than {@link
   * QueryCompilerConfig#maxPageSize()}, or, without a point in time, a {@code from + size} beyond
   * {@link QueryCompilerConfig#maxResultWindow()}, is rejected.
   *
   * @param criteria what to search for
   * @param index the index searched
   * @return the body and any warnings
   * @throws QueryCompileException when the criteria do not fit the index
   */
  public CompiledSearch compile(SearchCriteria criteria, IndexDefinition index) {
    List<String> warnings = new ArrayList<>();
    Map<String, Object> body = new TreeMap<>();
    body.put("query", query(criteria, index, warnings));
    List<Object> sort =
        new SortCompiler(config, index, warnings).compile(criteria.getSortingCriteriaList());
    final boolean pit = paging(criteria, sort, body, warnings);
    if (!sort.isEmpty()) {
      body.put("sort", sort);
    }
    Object trackTotalHits = config.trackTotalHits().toJson();
    if (trackTotalHits != null) {
      body.put("track_total_hits", trackTotalHits);
    }
    aggregationsAndHighlight(criteria, index, body, warnings);
    return new CompiledSearch(body, warnings, pit);
  }

  private void aggregationsAndHighlight(SearchCriteria criteria, IndexDefinition index,
      Map<String, Object> body, List<String> warnings) {
    if (criteria.getAggregationCount() > 0) {
      Map<String, Object> aggs = new AggregationCompiler(config, index, warnings)
          .compile(criteria.getAggregationList());
      if (!aggs.isEmpty()) {
        body.put("aggs", aggs);
      }
    }
    if (criteria.hasHighlight()) {
      Map<String, Object> highlight = new HighlightCompiler(config, index, warnings)
          .compile(criteria.getHighlight());
      if (highlight != null) {
        body.put("highlight", highlight);
      }
    }
  }

  /**
   * Compiles only the {@code query} clause, ignoring sorting, paging, aggregations and
   * highlighting.
   *
   * @param criteria what to search for
   * @param index the index searched
   * @return the query clause
   * @throws QueryCompileException when the criteria do not fit the index
   */
  public Map<String, Object> compileQuery(SearchCriteria criteria, IndexDefinition index) {
    return query(criteria, index, new ArrayList<>());
  }

  private Map<String, Object> query(SearchCriteria criteria, IndexDefinition index,
      List<String> warnings) {
    Objects.requireNonNull(index, "index");
    if (criteria.getSearchConditionCount() == 0) {
      return object("match_all", new TreeMap<>());
    }
    return new ConditionCompiler(config, index, warnings)
        .compileAny(criteria.getSearchConditionList());
  }

  private boolean paging(SearchCriteria criteria, List<Object> sort, Map<String, Object> body,
      List<String> warnings) {
    Pagination pagination = criteria.getPagination();
    int size = pagination.getSize();
    if (size == 0) {
      size = config.defaultSize();
    } else if (size < 0) {
      size = 0;
    }
    if (size > config.maxPageSize()) {
      throw new QueryCompileException(
          "pagination.size = " + size + " exceeds the maximum page size of "
              + config.maxPageSize() + " (QueryCompilerConfig.maxPageSize); request smaller"
              + " pages",
          null);
    }
    body.put("size", size);
    if (!pagination.getCursor().isEmpty()) {
      Map<String, Object> pit = new TreeMap<>();
      pit.put("id", pagination.getCursor());
      pit.put("keep_alive", pagination.getKeepAlive().isEmpty()
          ? JsonRestEngine.timeValue(config.defaultKeepAlive())
          : pagination.getKeepAlive());
      body.put("pit", pit);
      if (pagination.getSearchAfterCount() > 0) {
        body.put("search_after", PaginationSupport.searchAfter(pagination));
      }
      boolean hasTiebreaker = sort.stream()
          .anyMatch(clause -> clause instanceof Map && ((Map<?, ?>) clause).containsKey(SHARD_DOC));
      if (!hasTiebreaker && config.shardDocTiebreaker()) {
        sort.add(object(SHARD_DOC, object("order", "asc")));
      } else if (!hasTiebreaker) {
        warnings.add("no " + SHARD_DOC + " tiebreaker: pages are complete only if the sort ends"
            + " with a unique field");
      }
      return true;
    }
    int from = pagination.getFrom();
    if (from < 0) {
      throw new QueryCompileException("pagination.from must not be negative: " + from, null);
    }
    if ((long) from + size > config.maxResultWindow()) {
      throw new QueryCompileException(
          "from + size = " + ((long) from + size) + " exceeds the result window of "
              + config.maxResultWindow() + "; page deeper with a point in time (pagination.cursor"
              + " and search_after, see SearchPager)",
          null);
    }
    body.put("from", from);
    return false;
  }
}
