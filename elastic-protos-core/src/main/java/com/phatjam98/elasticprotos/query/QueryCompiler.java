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
 * <p>{@code NUM_RANGE} always emits both bounds, because proto3 cannot express an unset
 * {@code double}; use very small or very large values for an open-ended range.
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
   * {@code pit}/{@code search_after}) and {@code track_total_hits}.
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
    boolean pit = paging(criteria, sort, body, warnings);
    if (!sort.isEmpty()) {
      body.put("sort", sort);
    }
    if (config.trackTotalHits() != null) {
      body.put("track_total_hits", config.trackTotalHits());
    }
    return new CompiledSearch(body, warnings, pit);
  }

  /**
   * Compiles only the {@code query} clause, ignoring sorting and paging.
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
    ConditionCompiler conditions = new ConditionCompiler(config, index, warnings);
    List<Object> compiled = new ArrayList<>();
    for (SearchCondition condition : criteria.getSearchConditionList()) {
      compiled.add(conditions.compile(condition));
    }
    if (compiled.isEmpty()) {
      return object("match_all", new TreeMap<>());
    }
    if (compiled.size() == 1) {
      @SuppressWarnings("unchecked")
      Map<String, Object> single = (Map<String, Object>) compiled.get(0);
      return single;
    }
    Map<String, Object> bool = new TreeMap<>();
    bool.put("should", compiled);
    bool.put("minimum_should_match", 1);
    return object("bool", bool);
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
