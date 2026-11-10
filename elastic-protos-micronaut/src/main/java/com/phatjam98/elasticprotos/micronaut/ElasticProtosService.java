package com.phatjam98.elasticprotos.micronaut;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.phatjam98.elasticprotos.codec.ProtoDocumentCodec;
import com.phatjam98.elasticprotos.engine.BulkBuffer;
import com.phatjam98.elasticprotos.engine.BulkOp;
import com.phatjam98.elasticprotos.engine.BulkResult;
import com.phatjam98.elasticprotos.engine.Hit;
import com.phatjam98.elasticprotos.engine.IndexOptions;
import com.phatjam98.elasticprotos.engine.IndexResult;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.engine.SearchResult;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.mapping.ProtoMapper;
import com.phatjam98.elasticprotos.migration.IndexMigrator;
import com.phatjam98.elasticprotos.migration.MigrationPlan;
import com.phatjam98.elasticprotos.migration.MigrationResult;
import com.phatjam98.elasticprotos.proto.Pagination;
import com.phatjam98.elasticprotos.proto.SearchCriteria;
import com.phatjam98.elasticprotos.query.AggregationResults;
import com.phatjam98.elasticprotos.query.CompiledSearch;
import com.phatjam98.elasticprotos.query.CompositePager;
import com.phatjam98.elasticprotos.query.PaginationSupport;
import com.phatjam98.elasticprotos.query.QueryCompiler;
import com.phatjam98.elasticprotos.query.SearchPager;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repository-style access to protobuf messages stored in their index aliases.
 *
 * <p>Each message type is written to and read from the alias of its {@link IndexDefinition}, and
 * document ids come from the field the message marks as its id. Methods block and throw {@link
 * com.phatjam98.elasticprotos.engine.EngineException} on engine failures.
 */
@Singleton
@Requires(beans = SearchEngine.class)
public class ElasticProtosService {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticProtosService.class);

  private final SearchEngine engine;
  private final ProtoDocumentCodec codec;
  private final QueryCompiler compiler;
  private final IndexMigrator migrator;
  private final ElasticProtosConfiguration config;
  private volatile QueryCompiler pitCompiler;

  /**
   * Creates the service.
   *
   * @param engine the engine
   * @param codec the document codec
   * @param compiler the query compiler
   * @param migrator the index migrator
   * @param config the configuration, for the bulk buffer settings
   */
  public ElasticProtosService(SearchEngine engine, ProtoDocumentCodec codec,
      QueryCompiler compiler, IndexMigrator migrator, ElasticProtosConfiguration config) {
    this.engine = engine;
    this.codec = codec;
    this.compiler = compiler;
    this.migrator = migrator;
    this.config = config;
  }

  /** Returns the engine. */
  public SearchEngine engine() {
    return engine;
  }

  /**
   * Returns the index definition of a message type.
   *
   * @param type a generated message class
   * @return the definition, cached by the codec
   */
  public IndexDefinition definition(Class<? extends Message> type) {
    return codec.definition(ProtoMapper.descriptorOf(type));
  }

  /**
   * Writes a message to its alias, overwriting any document with the same id, without refresh.
   *
   * @param message the message
   * @return what happened
   */
  public IndexResult index(Message message) {
    return index(message, IndexOptions.defaults());
  }

  /**
   * Writes a message to its alias.
   *
   * @param message the message
   * @param options write options
   * @return what happened; the id is generated when the message has none
   */
  public IndexResult index(Message message, IndexOptions options) {
    IndexDefinition definition = codec.definition(message.getDescriptorForType());
    return engine.index(definition.alias(), codec.idOf(message).orElse(null),
        codec.toDocument(message, definition), options);
  }

  /**
   * Writes messages, possibly of several types, in one bulk request.
   *
   * @param messages the messages
   * @return the per-item outcome; item failures do not throw
   */
  public BulkResult bulkIndex(Collection<? extends Message> messages) {
    if (messages.isEmpty()) {
      return new BulkResult(false, List.of(), 0);
    }
    List<BulkOp> ops = new ArrayList<>(messages.size());
    for (Message message : messages) {
      ops.add(indexOp(message));
    }
    return engine.bulk(ops);
  }

  /**
   * Returns the bulk index operation for a message, for use with a {@link BulkBuffer}.
   *
   * @param message the message
   * @return an {@code INDEX} operation on the message's alias
   */
  public BulkOp indexOp(Message message) {
    IndexDefinition definition = codec.definition(message.getDescriptorForType());
    return BulkOp.index(definition.alias(), codec.idOf(message).orElse(null),
        codec.toDocument(message, definition));
  }

  /**
   * Reads one message by id through the realtime document API ({@code GET /{alias}/_doc/{id}}).
   * The read sees every acknowledged write and delete at once, without waiting for a refresh, so
   * a {@code get} right after {@link #index(Message)} or {@link #delete(String, Class)} reflects
   * it. The alias must point at a single index.
   *
   * @param id the document id
   * @param type the generated message class
   * @param <T> the message type
   * @return the message, or empty when there is no such document
   */
  public <T extends Message> Optional<T> get(String id, Class<T> type) {
    Objects.requireNonNull(id, "id");
    return engine.get(definition(type).alias(), id).map(hit -> codec.fromHit(hit, type));
  }

  /**
   * Deletes one document by id, without refresh: searches may still see it until the next
   * refresh, while {@link #get(String, Class)} does not.
   *
   * @param id the document id
   * @param type the generated message class whose alias holds it
   * @return true if it existed
   */
  public boolean delete(String id, Class<? extends Message> type) {
    return delete(id, type, IndexOptions.defaults());
  }

  /**
   * Deletes one document by id.
   *
   * @param id the document id
   * @param type the generated message class whose alias holds it
   * @param options only the refresh policy applies; {@link IndexOptions#refreshed()} makes the
   *     deletion visible to search before this returns
   * @return true if it existed
   */
  public boolean delete(String id, Class<? extends Message> type, IndexOptions options) {
    Objects.requireNonNull(id, "id");
    return engine.delete(definition(type).alias(), id, options);
  }

  /**
   * Runs one search and reads the hits back into messages, with the results of any aggregations
   * of the criteria and the highlights of the hits ({@link Hit#highlight()}).
   *
   * @param criteria what to search for, including sorting, pagination and aggregations
   * @param type the generated message class
   * @param <T> the message type
   * @return the page, with the pagination of the next one
   * @throws com.phatjam98.elasticprotos.query.QueryCompileException when the criteria do not fit
   *     the index
   */
  public <T extends Message> SearchPage<T> search(SearchCriteria criteria, Class<T> type) {
    IndexDefinition definition = definition(type);
    QueryCompiler effective = criteria.getPagination().getCursor().isEmpty()
        ? compiler
        : pitCompiler();
    CompiledSearch compiled = effective.compile(criteria, definition);
    logWarnings(definition, compiled);
    SearchResult result = engine.search(List.of(definition.alias()), compiled.body());
    List<T> items = new ArrayList<>(result.hits().size());
    for (Hit hit : result.hits()) {
      items.add(codec.fromHit(hit, type));
    }
    return new SearchPage<>(items, result.hits(), result.total(), result.totalExact(),
        PaginationSupport.next(criteria.getPagination(), result), compiled.warnings(),
        AggregationResults.of(result));
  }

  /**
   * Runs the aggregations of a search without returning hits: the page size is set to {@code -1},
   * which compiles to {@code size: 0}.
   *
   * @param criteria what to search for and the aggregations to compute; its pagination is
   *     replaced
   * @param type the generated message class whose alias is searched
   * @return the aggregation results, the total of matching documents and the compiler warnings
   * @throws com.phatjam98.elasticprotos.query.QueryCompileException when the criteria do not fit
   *     the index
   */
  public AggregationPage aggregate(SearchCriteria criteria, Class<? extends Message> type) {
    IndexDefinition definition = definition(type);
    SearchCriteria aggregationsOnly = criteria.toBuilder()
        .setPagination(Pagination.newBuilder().setSize(-1))
        .clearHighlight()
        .build();
    CompiledSearch compiled = compiler.compile(aggregationsOnly, definition);
    logWarnings(definition, compiled);
    SearchResult result = engine.search(List.of(definition.alias()), compiled.body());
    return new AggregationPage(AggregationResults.of(result), result.total(),
        compiled.warnings());
  }

  /**
   * Returns a pager over every bucket of a composite aggregation of the criteria, one page of
   * buckets per request; each request runs only that aggregation, without hits or highlights.
   * Compiler warnings are logged once, when the pager is created.
   *
   * @param criteria what to aggregate; it must hold a top-level composite aggregation named
   *     {@code name}
   * @param type the generated message class whose alias is searched
   * @param name the name of the composite aggregation
   * @return the pager; nothing is requested before its first {@code hasNext()}
   * @throws IllegalArgumentException when the criteria have no such composite aggregation
   * @throws com.phatjam98.elasticprotos.query.QueryCompileException when the criteria do not fit
   *     the index
   */
  public CompositePager composite(SearchCriteria criteria, Class<? extends Message> type,
      String name) {
    IndexDefinition definition = definition(type);
    CompositePager pager = CompositePager.open(engine, compiler, List.of(definition.alias()),
        criteria, definition, name);
    logWarnings(definition, compiler.compile(criteria.toBuilder()
        .setPagination(Pagination.newBuilder().setSize(-1)).clearHighlight().build(), definition));
    return pager;
  }

  private static void logWarnings(IndexDefinition definition, CompiledSearch compiled) {
    if (!compiled.warnings().isEmpty() && LOG.isWarnEnabled()) {
      LOG.warn("Search on {}: {}", definition.alias(), String.join("; ", compiled.warnings()));
    }
  }

  /**
   * Returns the compiler for point-in-time searches. With {@code
   * elastic-protos.query.shard-doc-tiebreaker: AUTO} the engine is asked once, on the first such
   * search, whether it supports {@code _shard_doc}; if not, the tiebreaker is turned off.
   */
  private QueryCompiler pitCompiler() {
    QueryCompiler resolved = pitCompiler;
    if (resolved == null) {
      resolved = compiler;
      if (compiler.config().shardDocTiebreaker()
          && config.getQuery().getShardDocTiebreaker()
              == ElasticProtosConfiguration.Query.ShardDocTiebreaker.AUTO
          && !engine.info().supportsShardDoc()) {
        resolved = QueryCompiler.of(compiler.config().withShardDocTiebreaker(false));
      }
      pitCompiler = resolved;
    }
    return resolved;
  }

  /**
   * Opens a point in time and returns a pager over every page of a search; read hits back with
   * {@link #codec()}. Close the pager when done.
   *
   * @param criteria what to search for; its pagination is replaced
   * @param type the generated message class
   * @param pageSize hits per page
   * @param <T> the message type
   * @return the pager
   */
  public <T extends Message> SearchPager pager(SearchCriteria criteria, Class<T> type,
      int pageSize) {
    IndexDefinition definition = definition(type);
    return SearchPager.open(engine, compiler, List.of(definition.alias()), criteria, definition,
        pageSize);
  }

  /**
   * Plans the migration of message types without changing anything.
   *
   * @param types generated message classes
   * @return the plan
   */
  @SafeVarargs
  public final MigrationPlan plan(Class<? extends Message>... types) {
    List<Descriptor> descriptors = new ArrayList<>(types.length);
    for (Class<? extends Message> type : types) {
      descriptors.add(ProtoMapper.descriptorOf(type));
    }
    return migrator.plan(descriptors);
  }

  /**
   * Creates or migrates the indices of message types.
   *
   * @param types generated message classes
   * @return the outcomes
   */
  @SafeVarargs
  public final MigrationResult bootstrap(Class<? extends Message>... types) {
    return migrator.apply(plan(types));
  }

  /**
   * Returns a new bulk buffer over the engine, configured from {@code elastic-protos.bulk.*}.
   * The caller owns it and must close it; build operations with {@link #indexOp(Message)}. Batch
   * results and engine failures go to the buffer's default listeners, which log them.
   *
   * @return the buffer
   */
  public BulkBuffer bulkBuffer() {
    return bulkBufferBuilder().build();
  }

  /**
   * Returns a new bulk buffer like {@link #bulkBuffer()} that reports every batch result to a
   * listener. Item failures arrive inside the {@link BulkResult}; engine failures for a whole
   * batch are logged.
   *
   * @param listener receives every batch result, on the thread that sent the batch
   * @return the buffer
   */
  public BulkBuffer bulkBuffer(Consumer<BulkResult> listener) {
    return bulkBufferBuilder().listener(listener).build();
  }

  /**
   * Returns a new bulk buffer like {@link #bulkBuffer()} that reports batch results and engine
   * failures to listeners. A batch whose engine call throws is handed to the error listener and
   * is not retried by the buffer; a failure of the final flush is also thrown from {@code
   * close()}.
   *
   * @param listener receives every batch result, on the thread that sent the batch
   * @param errorListener receives an immutable copy of the batch and the exception when the
   *     engine call for a batch throws
   * @return the buffer
   */
  public BulkBuffer bulkBuffer(Consumer<BulkResult> listener,
      BiConsumer<List<BulkOp>, Throwable> errorListener) {
    return bulkBufferBuilder().listener(listener).errorListener(errorListener).build();
  }

  /**
   * Returns a new bulk buffer that reports only the exception of a failed batch.
   *
   * @param listener receives every batch result, on the thread that sent the batch
   * @param errorListener receives the exception when the engine call for a batch throws
   * @return the buffer
   * @deprecated the failed operations are lost; use {@link #bulkBuffer(Consumer, BiConsumer)}
   */
  @Deprecated
  public BulkBuffer bulkBuffer(Consumer<BulkResult> listener,
      Consumer<Throwable> errorListener) {
    Objects.requireNonNull(errorListener, "errorListener");
    return bulkBuffer(listener, (batch, error) -> errorListener.accept(error));
  }

  private BulkBuffer.Builder bulkBufferBuilder() {
    ElasticProtosConfiguration.Bulk bulk = config.getBulk();
    return BulkBuffer.builder()
        .engine(engine)
        .maxActions(bulk.getActions())
        .maxBytes(bulk.getMaxBytes())
        .flushInterval(bulk.getFlushInterval());
  }

  /** Returns the document codec. */
  public ProtoDocumentCodec codec() {
    return codec;
  }
}
