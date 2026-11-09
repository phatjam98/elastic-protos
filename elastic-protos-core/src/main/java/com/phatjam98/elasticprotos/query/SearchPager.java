package com.phatjam98.elasticprotos.query;

import com.phatjam98.elasticprotos.engine.Pit;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.engine.SearchResult;
import com.phatjam98.elasticprotos.engine.rest.JsonRestEngine;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.proto.Pagination;
import com.phatjam98.elasticprotos.proto.SearchCriteria;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Iterates every page of a search over a point in time, so that deep paging is consistent and not
 * limited by the result window.
 *
 * <p>The point in time is opened when the pager is created and closed by {@link #close()}; use
 * try-with-resources. Each request carries the point-in-time id of the previous response and the
 * sort values of its last hit as {@code search_after}; the compiler adds the {@code _shard_doc}
 * tiebreaker when the engine supports it ({@link
 * com.phatjam98.elasticprotos.engine.EngineInfo#supportsShardDoc()}). OpenSearch before 3.3 has
 * no {@code _shard_doc}, so there the sort must end with a unique field, such as the document id,
 * for every hit to be seen exactly once. Iteration stops after a page with fewer hits than the
 * page size. Empty pages are never returned. The point in time requires Elasticsearch 7.10+ or
 * OpenSearch 2.4+.
 *
 * <p>Aggregations of the criteria are computed over every match, so they are sent with the first
 * request only: read them from the first page ({@link AggregationResults#of(SearchResult)}).
 * Highlighting applies to every page.
 *
 * <p>Not thread-safe.
 */
public final class SearchPager implements Iterator<SearchResult>, AutoCloseable {

  private final SearchEngine engine;
  private final QueryCompiler compiler;
  private final List<String> indices;
  private final SearchCriteria criteria;
  private final IndexDefinition index;
  private final int pageSize;
  private final Pit pit;
  private Pagination pagination;
  private SearchResult pending;
  private boolean done;
  private boolean closed;
  private boolean first = true;

  private SearchPager(SearchEngine engine, QueryCompiler compiler, List<String> indices,
      SearchCriteria criteria, IndexDefinition index, int pageSize) {
    if (pageSize <= 0) {
      throw new IllegalArgumentException("pageSize must be positive: " + pageSize);
    }
    Objects.requireNonNull(compiler, "compiler");
    if (pageSize > compiler.config().maxPageSize()) {
      throw new IllegalArgumentException("pageSize " + pageSize + " exceeds the maximum page size"
          + " of " + compiler.config().maxPageSize() + " (QueryCompilerConfig.maxPageSize)");
    }
    this.engine = Objects.requireNonNull(engine, "engine");
    this.compiler = engine.info().supportsShardDoc() || !compiler.config().shardDocTiebreaker()
        ? compiler
        : QueryCompiler.of(compiler.config().withShardDocTiebreaker(false));
    this.indices = List.copyOf(indices);
    this.criteria = Objects.requireNonNull(criteria, "criteria");
    this.index = Objects.requireNonNull(index, "index");
    this.pageSize = pageSize;
    this.pit = engine.openPit(this.indices, compiler.config().defaultKeepAlive());
    this.pagination = Pagination.newBuilder()
        .setSize(pageSize)
        .setCursor(pit.id())
        .setKeepAlive(JsonRestEngine.timeValue(pit.keepAlive()))
        .build();
  }

  /**
   * Opens a point in time and returns a pager over it.
   *
   * @param engine the engine to search
   * @param compiler compiles each page request; its default keep-alive is used for the PIT
   * @param indices the indices or aliases to search
   * @param criteria what to search for; its pagination is replaced
   * @param index the definition the criteria are compiled against
   * @param pageSize hits per page, positive and at most the compiler's maxPageSize
   * @return the pager; close it when done
   */
  public static SearchPager open(SearchEngine engine, QueryCompiler compiler, List<String> indices,
      SearchCriteria criteria, IndexDefinition index, int pageSize) {
    return new SearchPager(engine, compiler, indices, criteria, index, pageSize);
  }

  /**
   * Runs a search to the end, handing every page to a consumer, and closes the point in time.
   *
   * @param engine the engine to search
   * @param compiler compiles each page request
   * @param indices the indices or aliases to search
   * @param criteria what to search for
   * @param index the definition the criteria are compiled against
   * @param pageSize hits per page, positive and at most the compiler's maxPageSize
   * @param consumer receives each non-empty page in order
   */
  public static void forEachPage(SearchEngine engine, QueryCompiler compiler,
      List<String> indices, SearchCriteria criteria, IndexDefinition index, int pageSize,
      Consumer<SearchResult> consumer) {
    try (SearchPager pager = open(engine, compiler, indices, criteria, index, pageSize)) {
      pager.forEachRemaining(consumer);
    }
  }

  /** Returns whether another non-empty page is available, fetching it if needed. */
  @Override
  public boolean hasNext() {
    if (pending != null) {
      return true;
    }
    if (done || closed) {
      return false;
    }
    SearchCriteria.Builder builder = criteria.toBuilder().setPagination(pagination);
    if (!first) {
      // aggregations cover every match already on the first page; highlighting stays per page
      builder.clearAggregation();
    }
    first = false;
    SearchCriteria request = builder.build();
    SearchResult result = engine.search(indices, compiler.compile(request, index).body());
    pagination = PaginationSupport.next(pagination, result);
    if (result.hits().size() < pageSize) {
      done = true;
    }
    if (result.hits().isEmpty()) {
      return false;
    }
    pending = result;
    return true;
  }

  /** Returns the next non-empty page. */
  @Override
  public SearchResult next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more pages");
    }
    SearchResult result = pending;
    pending = null;
    return result;
  }

  /** Returns the pagination the next request will use, including the latest PIT id. */
  public Pagination pagination() {
    return pagination;
  }

  /** Closes the point in time; later calls do nothing. */
  @Override
  public void close() {
    if (!closed) {
      closed = true;
      engine.closePit(new Pit(pagination.getCursor(), pit.keepAlive()));
    }
  }
}
