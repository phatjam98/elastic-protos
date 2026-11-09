package com.phatjam98.elasticprotos.engine;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The operations elastic-protos needs from a search engine, expressed in plain JSON-shaped maps so
 * that Elasticsearch and OpenSearch can be served by thin adapters.
 *
 * <p>Every method blocks until the engine answers and throws {@link EngineException} on failure.
 */
public interface SearchEngine extends AutoCloseable {

  /** Returns the product, version and capabilities of the cluster. */
  EngineInfo info();

  /** Returns whether an index or alias exists. */
  boolean indexExists(String aliasOrIndex);

  /**
   * Creates an index with settings, mappings and aliases in one atomic request.
   *
   * @param name the concrete index name
   * @param settings index settings, possibly empty
   * @param mappings the {@code mappings} object
   * @param aliases aliases to attach, possibly empty
   */
  void createIndex(
      String name, Map<String, Object> settings, Map<String, Object> mappings, Set<String> aliases);

  /** Deletes a concrete index. */
  void deleteIndex(String index);

  /**
   * Returns the {@code mappings} object of one concrete index or of the single index behind an
   * alias.
   *
   * @throws EngineException with {@code NOT_FOUND} if nothing matches, or {@code CONFLICT} if an
   *     alias spans several indices
   */
  Map<String, Object> getMappings(String aliasOrIndex);

  /**
   * Returns the {@code settings} object of one concrete index or the single index behind an alias.
   */
  Map<String, Object> getSettings(String aliasOrIndex);

  /**
   * Adds fields to the mapping of an index. Existing fields can only be extended, never changed.
   */
  void putMappings(String index, Map<String, Object> mappings);

  /**
   * Returns the aliases of every concrete index that an alias or index name resolves to.
   *
   * @return concrete index name to its aliases; empty when nothing matches
   */
  Map<String, Set<String>> getAliases(String aliasOrIndex);

  /** Applies alias actions atomically, in one request. */
  void updateAliases(List<AliasAction> actions);

  /** Copies documents between indices and waits for completion. */
  ReindexResult reindex(ReindexRequest request);

  /**
   * Rewrites the matching documents of an index in place and waits for completion. Each rewritten
   * document is re-indexed under the current mapping and gets a new version and sequence number.
   *
   * @param request the index, query and options
   * @return the outcome; {@link ReindexResult#updated()} counts the rewritten documents
   */
  ReindexResult updateByQuery(UpdateByQueryRequest request);

  /**
   * Updates dynamic settings of an index or of the indices behind an alias.
   *
   * @param index the index or alias
   * @param settings the settings, either as flat keys ({@code "index.refresh_interval": "5s"}) or
   *     nested ({@code {"index": {"refresh_interval": "5s"}}}); sent unchanged
   */
  void putSettings(String index, Map<String, Object> settings);

  /** Blocks or unblocks writes to an index. */
  void setWriteBlock(String index, boolean blocked);

  /**
   * Writes one document.
   *
   * <p>With {@link IndexOptions#ifSeqNo()} and {@link IndexOptions#ifPrimaryTerm()} the write
   * only succeeds while the stored document has that sequence number and primary term, and
   * otherwise fails with {@link EngineException.Kind#CONFLICT}.
   *
   * @param index the index or alias
   * @param id the document id, or null to have one generated
   * @param document the document body
   * @param options write options
   * @return what happened, with the sequence number and primary term of the write
   */
  IndexResult index(String index, String id, Map<String, Object> document, IndexOptions options);

  /** Merges a partial document into an existing document. */
  IndexResult update(String index, String id, Map<String, Object> partial);

  /**
   * Reads one document by id through the realtime document API ({@code GET /{index}/_doc/{id}}),
   * so the result reflects every acknowledged write and delete without waiting for a refresh.
   *
   * @param index a concrete index, or an alias that resolves to exactly one index
   * @param id the document id
   * @return the document as a hit without score or sort values but with its version, sequence
   *     number and primary term, or empty when the document or the index does not exist
   */
  Optional<Hit> get(String index, String id);

  /**
   * Deletes a document without refresh.
   *
   * <p>The default implementation calls {@link #delete(String, String, IndexOptions)} with {@link
   * IndexOptions#defaults()}.
   *
   * @param index the index or alias
   * @param id the document id
   * @return true if it existed
   */
  default boolean delete(String index, String id) {
    return delete(index, id, IndexOptions.defaults());
  }

  /**
   * Deletes a document.
   *
   * @param index the index or alias
   * @param id the document id
   * @param options {@link IndexOptions#refresh()}: when the deletion becomes visible to search;
   *     {@link IndexOptions#ifSeqNo()} and {@link IndexOptions#ifPrimaryTerm()}: delete only
   *     while the document has them, else fail with {@link EngineException.Kind#CONFLICT};
   *     {@code createOnly} is ignored
   * @return true if it existed
   */
  boolean delete(String index, String id, IndexOptions options);

  /** Executes operations in bulk. */
  BulkResult bulk(List<BulkOp> operations);

  /**
   * Runs a search.
   *
   * @param indices indices or aliases; ignored when the body carries a point in time
   * @param body the search body
   * @return the result
   */
  SearchResult search(List<String> indices, Map<String, Object> body);

  /**
   * Counts the documents in an index or alias exactly.
   *
   * <p>The default implementation runs {@link #search} with {@code {"size": 0,
   * "track_total_hits": true}}, so it sees only documents visible to search; call {@link
   * #refresh} first to include recent writes.
   *
   * @param aliasOrIndex the index or alias to count
   * @return the exact number of matching documents
   */
  default long count(String aliasOrIndex) {
    return search(List.of(aliasOrIndex), Map.of("size", 0, "track_total_hits", true)).total();
  }

  /** Opens a point in time over indices. */
  Pit openPit(List<String> indices, Duration keepAlive);

  /** Closes a point in time. */
  void closePit(Pit pit);

  /** Makes recent writes to an index visible to search. */
  void refresh(String index);

  /** Returns cluster health. */
  ClusterHealth health();

  /** Releases the underlying connections. */
  @Override
  void close();
}
