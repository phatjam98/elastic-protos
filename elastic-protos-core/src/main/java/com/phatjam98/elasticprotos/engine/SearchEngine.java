package com.phatjam98.elasticprotos.engine;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
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

  /**
   * Blocks or unblocks writes to an index. Blocking waits for writes already in flight on the
   * index to finish ({@code PUT /<index>/_block/write} on the engines); both directions show in
   * {@link #getSettings} as {@code index.blocks.write}.
   */
  void setWriteBlock(String index, boolean blocked);

  /**
   * Returns a sequence number at or below which every document of an index was last written before
   * this call: the minimum over its shards of {@code seq_no.max_seq_no} from the index stats. A
   * document whose {@code _seq_no} is above it may have been written since; sequence numbers are
   * per shard, so documents of other shards may be above it too without having changed.
   *
   * <p>The default returns {@code -1}, which makes callers treat every document as possibly
   * changed.
   *
   * @param index a concrete index
   * @return the checkpoint, or -1 when unknown
   */
  default long seqNoCheckpoint(String index) {
    return -1;
  }

  /**
   * Returns the global checkpoint of an index: the lowest {@code seq_no.global_checkpoint} over the
   * primaries of all its shards, from the index stats.
   *
   * <p>Implementations promise more than {@link #seqNoCheckpoint}: on every shard, every operation
   * with a sequence number at or below the returned value was processed by every in-sync copy
   * before this call returned (so, with document replication, a refresh after the call makes its
   * effect visible to every search; segment replication gives no such guarantee for replicas),
   * and no operation started later gets a sequence number at or below it, even after a
   * primary fails over. The value may be -1 (a shard without operations). An implementation that
   * cannot promise this, or that misses a shard (an unassigned primary), returns empty.
   *
   * <p>The zero-downtime reindex uses it to read only the documents written after the copy
   * started instead of every document; the default returns empty, which keeps the full scan.
   *
   * @param index a concrete index
   * @return the global checkpoint, or empty when unknown
   */
  default OptionalLong globalCheckpoint(String index) {
    return OptionalLong.empty();
  }

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
   * @throws IllegalArgumentException when the write is conditional but has no id
   */
  IndexResult index(String index, String id, Map<String, Object> document, IndexOptions options);

  /**
   * Rejects a conditional write without a document id, for implementations of {@link #index}.
   *
   * @param id the document id, or null
   * @param options the write options
   * @throws IllegalArgumentException when {@code options} is conditional and {@code id} is null
   */
  static void checkConditionalId(String id, IndexOptions options) {
    if (id == null && options.conditional()) {
      throw new IllegalArgumentException(
          "a conditional write (ifSeqNo/ifPrimaryTerm) needs a document id");
    }
  }

  /** The {@code retry_on_conflict} of {@link #update(String, String, Map)}. */
  int DEFAULT_UPDATE_RETRIES = 3;

  /**
   * Merges a partial document into an existing document, retrying on a version conflict up to
   * {@link #DEFAULT_UPDATE_RETRIES} times.
   *
   * <p>The engine reads the document and writes the merge conditionally on what it read; a write
   * between the two (another writer, an {@code _update_by_query} of a backfill, or the alias swap
   * of a zero-downtime reindex, after which the write resolves against the new index) fails it
   * with 409. The merge carries no condition of the caller's, so applying it again to the newest
   * version is what was asked for; implementations retry in the engine ({@code
   * retry_on_conflict}).
   */
  IndexResult update(String index, String id, Map<String, Object> partial);

  /**
   * Merges a partial document into an existing document, retrying on a version conflict up to
   * {@code retryOnConflict} times ({@code retry_on_conflict}); 0 fails at the first conflict.
   *
   * <p>The default ignores {@code retryOnConflict} and calls {@link #update(String, String,
   * Map)}, so on an implementation that does not override it, {@code 0} does not make the update
   * fail at the first conflict and the count is not validated; {@code JsonRestEngine} (both
   * adapters) honours it and rejects a negative count.
   *
   * @param index the index or alias
   * @param id the document id
   * @param partial the fields to merge
   * @param retryOnConflict how many times to retry after a version conflict, 0 or more
   * @return what happened
   */
  default IndexResult update(String index, String id, Map<String, Object> partial,
      int retryOnConflict) {
    return update(index, id, partial);
  }

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
   *     while the document has them, else fail with {@link EngineException.Kind#CONFLICT}, which
   *     includes a conditional delete of a missing document (Elasticsearch and OpenSearch both
   *     answer 409 rather than 404); {@code createOnly} is ignored
   * @return true if it existed; a conditional delete never returns false
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
