package com.phatjam98.elasticprotos.query;

import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.engine.SearchResult;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.proto.Aggregation;
import com.phatjam98.elasticprotos.proto.Pagination;
import com.phatjam98.elasticprotos.proto.SearchCriteria;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Iterates every page of buckets of a composite aggregation, passing the {@code after_key} of each
 * page as the {@code after} of the next.
 *
 * <p>Each request carries the criteria's conditions and only the named composite aggregation (with
 * its sub-aggregations), no hits and no highlighting. Iteration stops when the engine returns a
 * page without buckets or without an {@code after_key}; empty pages are never returned. A page
 * that holds fewer buckets than the composite's size is not taken as the last one, since the
 * engines do not promise it, so the end costs one more request. An {@code after} already set on the
 * composite aggregation is where the first page starts, which resumes an iteration from {@link
 * #afterKey()}.
 *
 * <p>Each page is a separate search: unlike {@link SearchPager}, no point in time holds the data
 * still, so documents written during the iteration may or may not be counted. Not thread-safe.
 *
 * <pre>{@code
 * Aggregation.Builder pairs = Aggs.composite("pairs",
 *     Aggs.termsSource("firm", "firm_id"), Aggs.termsSource("type", "type"));
 * pairs.getCompositeBuilder().setSize(500);
 * SearchCriteria criteria = Criteria.builder().aggregate(pairs).build();
 * CompositePager.forEachBucket(engine, compiler, List.of("documents"), criteria, documents,
 *     "pairs", bucket -> System.out.println(bucket.compositeKey() + " " + bucket.docCount()));
 * }</pre>
 */
public final class CompositePager implements Iterator<List<AggregationResults.Bucket>> {

  private final SearchEngine engine;
  private final QueryCompiler compiler;
  private final List<String> indices;
  private final IndexDefinition index;
  private final String name;
  private SearchCriteria request;
  private Map<String, Object> afterKey;
  private List<AggregationResults.Bucket> pending;
  private boolean done;

  private CompositePager(SearchEngine engine, QueryCompiler compiler, List<String> indices,
      SearchCriteria criteria, IndexDefinition index, String name) {
    this.engine = Objects.requireNonNull(engine, "engine");
    this.compiler = Objects.requireNonNull(compiler, "compiler");
    this.indices = List.copyOf(indices);
    this.index = Objects.requireNonNull(index, "index");
    this.name = Objects.requireNonNull(name, "name");
    Aggregation composite = Objects.requireNonNull(criteria, "criteria").getAggregationList()
        .stream()
        .filter(aggregation -> aggregation.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("The criteria have no top-level"
            + " aggregation named '" + name + "'"));
    if (composite.getKindCase() != Aggregation.KindCase.COMPOSITE) {
      throw new IllegalArgumentException("Aggregation '" + name + "' is a "
          + composite.getKindCase() + " aggregation, not a composite one");
    }
    this.request = criteria.toBuilder()
        .clearAggregation()
        .addAggregation(composite)
        .clearHighlight()
        .setPagination(Pagination.newBuilder().setSize(-1))
        .build();
  }

  /**
   * Returns a pager over the buckets of a composite aggregation; nothing is requested before the
   * first {@link #hasNext()}.
   *
   * @param engine the engine to search
   * @param compiler compiles each page request
   * @param indices the indices or aliases to search
   * @param criteria what to aggregate; its pagination, highlighting and other aggregations are
   *     ignored
   * @param index the definition the criteria are compiled against
   * @param name the name of a top-level composite aggregation of the criteria
   * @return the pager
   * @throws IllegalArgumentException when the criteria have no such composite aggregation
   */
  public static CompositePager open(SearchEngine engine, QueryCompiler compiler,
      List<String> indices, SearchCriteria criteria, IndexDefinition index, String name) {
    return new CompositePager(engine, compiler, indices, criteria, index, name);
  }

  /**
   * Hands every bucket of a composite aggregation to a consumer, page by page.
   *
   * @param engine the engine to search
   * @param compiler compiles each page request
   * @param indices the indices or aliases to search
   * @param criteria what to aggregate
   * @param index the definition the criteria are compiled against
   * @param name the name of a top-level composite aggregation of the criteria
   * @param consumer receives each bucket in order
   */
  public static void forEachBucket(SearchEngine engine, QueryCompiler compiler,
      List<String> indices, SearchCriteria criteria, IndexDefinition index, String name,
      Consumer<AggregationResults.Bucket> consumer) {
    open(engine, compiler, indices, criteria, index, name)
        .forEachRemaining(page -> page.forEach(consumer));
  }

  /** Returns whether another non-empty page is available, fetching it if needed. */
  @Override
  public boolean hasNext() {
    if (pending != null) {
      return true;
    }
    if (done) {
      return false;
    }
    SearchResult result = engine.search(indices, compiler.compile(request, index).body());
    AggregationResults results = AggregationResults.of(result);
    List<AggregationResults.Bucket> buckets = results.buckets(name);
    Optional<Map<String, Object>> after = results.afterKey(name);
    if (buckets.isEmpty() || after.isEmpty()) {
      done = true;
    }
    if (after.isPresent()) {
      afterKey = after.get();
      Aggregation.Builder composite = request.getAggregation(0).toBuilder();
      request = request.toBuilder().setAggregation(0, Aggs.after(composite, afterKey)).build();
    }
    if (buckets.isEmpty()) {
      return false;
    }
    pending = buckets;
    return true;
  }

  /** Returns the buckets of the next non-empty page. */
  @Override
  public List<AggregationResults.Bucket> next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more pages");
    }
    List<AggregationResults.Bucket> page = pending;
    pending = null;
    return page;
  }

  /**
   * Returns the after key of the latest page: set it as the composite's {@code after} ({@link
   * Aggs#after}) to resume after that page later.
   *
   * @return the after key, or empty before the first page or when the engine returned none
   */
  public Optional<Map<String, Object>> afterKey() {
    return Optional.ofNullable(afterKey);
  }
}
