package com.phatjam98.elasticprotos.testkit;

import com.phatjam98.elasticprotos.engine.AliasAction;
import com.phatjam98.elasticprotos.engine.BulkOp;
import com.phatjam98.elasticprotos.engine.BulkResult;
import com.phatjam98.elasticprotos.engine.ClusterHealth;
import com.phatjam98.elasticprotos.engine.EngineException;
import com.phatjam98.elasticprotos.engine.EngineException.Kind;
import com.phatjam98.elasticprotos.engine.EngineInfo;
import com.phatjam98.elasticprotos.engine.EngineProduct;
import com.phatjam98.elasticprotos.engine.Hit;
import com.phatjam98.elasticprotos.engine.IndexOptions;
import com.phatjam98.elasticprotos.engine.IndexResult;
import com.phatjam98.elasticprotos.engine.Pit;
import com.phatjam98.elasticprotos.engine.ReindexRequest;
import com.phatjam98.elasticprotos.engine.ReindexResult;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.engine.SearchResult;
import com.phatjam98.elasticprotos.engine.UpdateByQueryRequest;
import com.phatjam98.elasticprotos.util.Json;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A deterministic, single-threaded {@link SearchEngine} over plain maps, for tests.
 *
 * <p>It models what the migrator and simple repository tests need, and behaves like a real engine
 * where that matters:
 *
 * <ul>
 *   <li>Names resolve like Elasticsearch: a concrete index name, or an alias that may point at
 *       several indices. Reads of mappings or settings through a multi-index alias fail with
 *       {@code CONFLICT}; unknown names fail with {@code NOT_FOUND}.
 *   <li>{@link #getMappings} returns mappings the way the engine does: {@code dynamic} as a string
 *       and objects with {@code properties} without {@code type: object}. {@link #getSettings}
 *       returns {@code {"index": {...}}} with string scalars, including default shard and replica
 *       counts and {@code blocks.write} when blocked.
 *   <li>{@link #putMappings} deep-merges {@code properties} and replaces {@code _meta} as a whole
 *       when the update carries one, like both engines; with {@link #rejectTypeChanges(boolean)}
 *       (on by default) it refuses to change an existing field's type.
 *   <li>{@link #updateAliases} validates every action before applying any.
 *   <li>Writes to a write-blocked index fail with {@code BLOCKED} and status 403. Calls are
 *       sequential, so {@link #setWriteBlock} has no in-flight writes to wait for, unlike the
 *       engines' {@code _block/write}.
 *   <li>Every write takes the next value of one global sequence number counter (the primary term
 *       is always 1) and increments the document's version. {@link #get} always returns version,
 *       sequence number and primary term; {@link #search} returns the version when the body asks
 *       {@code "version": true} and the other two when it asks {@code "seq_no_primary_term":
 *       true}. Writes and bulk items with {@code ifSeqNo}/{@code ifPrimaryTerm} that do not match
 *       the stored document fail with {@code CONFLICT} and status 409.
 *   <li>{@link #reindex} copies from a snapshot of the source taken when the call starts, honours
 *       {@code source.query} (the same queries as search), and with {@link
 *       ReindexRequest.VersionType#EXTERNAL} keeps the source versions and skips documents whose
 *       destination version (or tombstone version) is equal or higher: counted as {@code
 *       versionConflicts} when the request proceeds on conflicts, otherwise the copy stops at the
 *       first conflict and reports it as a failure. {@code total} is then still the number of
 *       snapshot documents, so {@code accounted()} is false, as on the engines; unlike them, which
 *       finish writing the current batch, the copy stops right there.
 *   <li>A delete bumps the version and keeps it as a tombstone, so a later write of the same id
 *       continues from it and an external-version copy onto it is a version conflict.
 *   <li>{@link #updateByQuery} snapshots the sequence numbers of the matching documents and
 *       rewrites each one, bumping version and sequence number; a document changed or deleted
 *       since the snapshot is a version conflict, counted when the request proceeds on conflicts
 *       and otherwise reported as one 409 failure that stops the run. {@link #putSettings} merges
 *       into the stored settings ({@code blocks.write} sets the write block).
 *   <li>{@link #openPit} snapshots the documents, versions and sequence numbers of its indices, so
 *       a search with that point in time sees them as they were when it was opened. {@link
 *       #expirePits()} drops every open point in time, after which searches with one fail with
 *       {@code NOT_FOUND}.
 * </ul>
 *
 * <p>Test hooks: {@link #duringReindex} runs after a reindex took its snapshot and before it
 * copies, and {@link #duringUpdateByQuery} likewise for an update by query, so a spec can write
 * concurrently; {@link #afterAliasUpdate} runs after {@link #updateAliases} applied its actions.
 *
 * <p>Limits: documents are never validated against mappings; tombstones never expire unless
 * {@link #gcDeletes(Duration)} sets a lifetime or {@link #expireTombstones()} drops them (the
 * engines' {@code index.gc_deletes}, 60 s by default), after which a write of the id starts again
 * at version 1; every write is immediately visible
 * (refresh is a no-op); writes to unknown indices fail instead of auto-creating them; {@link
 * #reindex} requires the destination to exist and supports only a tiny stand-in for Painless
 * (one statement per line, {@code ;} optional, blank and {@code //} lines ignored):
 * {@code ctx._source.<field> = params.<name>}, {@code ctx._source.remove('<field>')} and {@code
 * ctx._id = params.<name>};
 * {@link #search} supports only the queries {@code match_all}, {@code term} and {@code terms} on
 * one field (dotted paths walk into objects and arrays), {@code ids}, a top-level {@code range}
 * on {@code _seq_no} with {@code gt}, {@code gte}, {@code lt} and {@code lte} (also inside
 * {@code bool} in searches), and {@code bool} with {@code filter}, {@code must}, {@code
 * must_not} and {@code should} (with {@code minimum_should_match}, default 1 when there is no
 * filter or must clause, else 0) over those, plus {@code from}, {@code size}, {@code
 * track_total_hits}, {@code pit} and {@code search_after}. Hits come in insertion order, or by
 * sequence number when the sort starts with {@code _seq_no}; when the body has a {@code sort},
 * whatever else it says, each hit's
 * only sort value is its position among the matches, and {@code search_after} resumes after that
 * position, which is enough for point-in-time paging. A {@code highlight} in the body is
 * accepted and ignored: hits never carry highlights. Any other query fails with {@code
 * BAD_REQUEST}.
 *
 * <p>Aggregations ({@code aggs} or {@code aggregations}) run over every match, not just the page:
 * {@code terms} ({@code field}, {@code size}, {@code min_doc_count}, {@code missing} and {@code
 * order} by {@code _count}, {@code _key} or a metric sub-aggregation path, alone or compound), the
 * metrics {@code min}, {@code max}, {@code avg}, {@code sum}, {@code value_count} and {@code
 * cardinality} (exact, unlike the engines' approximate count, over numbers or RFC 3339 dates),
 * {@code nested} (one element per nested object) and {@code reverse_nested} (each enclosing object
 * or root document counted once), with sub-aggregations as the query compiler emits them: one level
 * of user aggregations plus the nested wrappers. Keys and formatted values follow the engines,
 * using the index mappings: terms on {@code boolean} fields have keys {@code 1}/{@code 0} with
 * {@code key_as_string} {@code true}/{@code false}; terms on {@code date} fields have epoch
 * millisecond keys with {@code key_as_string} such as {@code 2024-06-30T08:00:00.000Z}, the form
 * {@code value_as_string} of date metrics takes too; a {@code .keyword} sub-field of a text field
 * reads the text value. {@code date_histogram}, {@code histogram}, {@code range}, {@code
 * date_range} and any other aggregation fail with {@code BAD_REQUEST}. Terms buckets tie on their
 * count by key, ascending, like the engines.
 *
 * <p>Every call is recorded in {@link #log()} as the operation name and its target, for example
 * {@code setWriteBlock documents-v1=true}, so specs can assert sequences.
 */
public class InMemorySearchEngine implements SearchEngine {

  /** The engine never fails over, so every write has primary term 1. */
  private static final Long PRIMARY_TERM = 1L;
  /** The sequence number of the document a query is matched against, for range on _seq_no. */
  private static final ThreadLocal<Long> CURRENT_SEQ_NO = new ThreadLocal<>();
  private static final Pattern ASSIGN =
      Pattern.compile("ctx\\._source\\.([A-Za-z0-9_]+)\\s*=\\s*params\\.([A-Za-z0-9_]+)\\s*;?");
  private static final Pattern REMOVE =
      Pattern.compile("ctx\\._source\\.remove\\(\\s*'([^']+)'\\s*\\)\\s*;?");
  private static final Pattern ASSIGN_ID =
      Pattern.compile("ctx\\._id\\s*=\\s*params\\.([A-Za-z0-9_]+)\\s*;?");

  private final Map<String, IndexState> indices = new TreeMap<>();
  private final Map<String, Map<String, IndexState>> pits = new LinkedHashMap<>();
  private final List<String> log = new ArrayList<>();
  private EngineInfo info = new EngineInfo(EngineProduct.ELASTICSEARCH, "9.0.0", "in-memory");
  private int dataNodes = 1;
  private boolean rejectTypeChanges = true;
  private long generatedIds;
  private long seqNo = -1;
  private boolean globalCheckpoints = true;
  private long globalCheckpointLag;
  private Consumer<ReindexRequest> duringReindex = request -> {};
  private Runnable afterAliasUpdate = () -> {};
  private Consumer<UpdateByQueryRequest> duringUpdateByQuery = request -> {};
  private Duration gcDeletes;

  /** Creates an empty engine. */
  public InMemorySearchEngine() {}

  // ---------------------------------------------------------------- configuration and inspection

  /** Sets what {@link #info()} returns. */
  public InMemorySearchEngine engineInfo(EngineInfo value) {
    this.info = Objects.requireNonNull(value);
    return this;
  }

  /** Sets the data node count reported by {@link #health()}. */
  public InMemorySearchEngine dataNodes(int value) {
    this.dataNodes = value;
    return this;
  }

  /** Sets whether {@link #putMappings} refuses to change the type of an existing field. */
  public InMemorySearchEngine rejectTypeChanges(boolean value) {
    this.rejectTypeChanges = value;
    return this;
  }

  /**
   * Sets a hook that {@link #reindex} runs after it took the snapshot of the source and before it
   * copies, for example to write to the source while a reindex is in flight; null removes it.
   */
  public InMemorySearchEngine duringReindex(Consumer<ReindexRequest> hook) {
    this.duringReindex = hook == null ? request -> {} : hook;
    return this;
  }

  /**
   * Sets a hook that {@link #updateByQuery} runs after it took the sequence numbers of the
   * matching documents and before it rewrites them, for example to change one of them so that it
   * conflicts; null removes it.
   */
  public InMemorySearchEngine duringUpdateByQuery(Consumer<UpdateByQueryRequest> hook) {
    this.duringUpdateByQuery = hook == null ? request -> {} : hook;
    return this;
  }

  /**
   * Returns the version a deleted document left behind as its tombstone, or null when the id has
   * none (it was never deleted, or was written again since).
   */
  public Long tombstoneVersion(String index, String id) {
    return tombstone(concrete(index), id);
  }

  /**
   * Sets a hook that {@link #updateAliases} runs after it applied its actions; null removes it.
   */
  public InMemorySearchEngine afterAliasUpdate(Runnable hook) {
    this.afterAliasUpdate = hook == null ? () -> {} : hook;
    return this;
  }

  /**
   * Drops every open point in time, as if its keep-alive had run out; searches with one then
   * fail with {@code NOT_FOUND}.
   */
  public InMemorySearchEngine expirePits() {
    pits.clear();
    return this;
  }

  /** Returns the ids of the open points in time. */
  public Set<String> openPits() {
    return new LinkedHashSet<>(pits.keySet());
  }

  /**
   * Sets how long a delete's version tombstone lives, like {@code index.gc_deletes}; null (the
   * default) keeps tombstones forever.
   */
  public InMemorySearchEngine gcDeletes(Duration lifetime) {
    this.gcDeletes = lifetime;
    return this;
  }

  /** Drops every version tombstone, as if {@code index.gc_deletes} had passed for all of them. */
  public InMemorySearchEngine expireTombstones() {
    for (IndexState state : indices.values()) {
      state.tombstones.clear();
      state.tombstoneTimes.clear();
    }
    return this;
  }

  /**
   * Returns the global sequence number counter: every document written after this call has a
   * higher {@code _seq_no}.
   */
  @Override
  public long seqNoCheckpoint(String index) {
    record("seqNoCheckpoint", index);
    concrete(index);
    return seqNo;
  }

  /**
   * Returns the global sequence number counter less the {@link #globalCheckpointLag}, or empty
   * after {@link #globalCheckpoints(boolean) globalCheckpoints(false)}. Every operation is
   * processed at once, so every document written at or below it is visible.
   */
  @Override
  public OptionalLong globalCheckpoint(String index) {
    record("globalCheckpoint", index);
    concrete(index);
    return globalCheckpoints ? OptionalLong.of(seqNo - globalCheckpointLag) : OptionalLong.empty();
  }

  /** Sets whether {@link #globalCheckpoint} reports a checkpoint (default) or none. */
  public InMemorySearchEngine globalCheckpoints(boolean value) {
    this.globalCheckpoints = value;
    return this;
  }

  /**
   * Lowers what {@link #globalCheckpoint} reports by {@code lag} sequence numbers, the way the
   * lowest checkpoint over several shards lies below documents of the other shards: documents just
   * below the counter then count as written after it, which is safe.
   */
  public InMemorySearchEngine globalCheckpointLag(long lag) {
    this.globalCheckpointLag = lag;
    return this;
  }

  /** Returns every concrete index with its aliases, sorted by name. */
  public Map<String, Set<String>> indices() {
    Map<String, Set<String>> result = new TreeMap<>();
    indices.forEach((name, state) -> result.put(name, new TreeSet<>(state.aliases)));
    return result;
  }

  /** Returns a copy of the documents of a concrete index, in insertion order. */
  public Map<String, Map<String, Object>> documents(String index) {
    Map<String, Map<String, Object>> copy = new LinkedHashMap<>();
    concrete(index).docs.forEach((id, source) -> copy.put(id, Json.sortedCopy(source)));
    return copy;
  }

  /** Returns whether writes to a concrete index are blocked. */
  public boolean isWriteBlocked(String index) {
    return concrete(index).writeBlocked;
  }

  /** Returns every recorded operation, oldest first. */
  public List<String> log() {
    return Collections.unmodifiableList(log);
  }

  /** Forgets the recorded operations. */
  public void clearLog() {
    log.clear();
  }

  // ---------------------------------------------------------------- cluster

  @Override
  public EngineInfo info() {
    record("info", "");
    return info;
  }

  @Override
  public ClusterHealth health() {
    record("health", "");
    return new ClusterHealth("green", dataNodes, dataNodes);
  }

  @Override
  public void close() {
    record("close", "");
  }

  // ---------------------------------------------------------------- indices

  @Override
  public boolean indexExists(String aliasOrIndex) {
    record("indexExists", aliasOrIndex);
    return !resolve(aliasOrIndex).isEmpty();
  }

  @Override
  public void createIndex(
      String name, Map<String, Object> settings, Map<String, Object> mappings,
      Set<String> aliases) {
    record("createIndex", name + (aliases.isEmpty() ? "" : " aliases=" + new TreeSet<>(aliases)));
    if (indices.containsKey(name) || isAlias(name)) {
      throw new EngineException(Kind.ALREADY_EXISTS, 400,
          "resource_already_exists_exception: index [" + name + "] already exists");
    }
    for (String alias : aliases) {
      if (indices.containsKey(alias)) {
        throw new EngineException(Kind.BAD_REQUEST, 400,
            "invalid_alias_name_exception: an index exists with the same name as the alias ["
                + alias + "]");
      }
    }
    IndexState state = new IndexState();
    state.settings = flattenSettings(settings);
    state.mappings = Json.sortedCopy(mappings);
    state.aliases.addAll(aliases);
    indices.put(name, state);
  }

  @Override
  public void deleteIndex(String index) {
    record("deleteIndex", index);
    if (!indices.containsKey(index)) {
      throw notFound(index);
    }
    indices.remove(index);
  }

  @Override
  public Map<String, Object> getMappings(String aliasOrIndex) {
    record("getMappings", aliasOrIndex);
    return engineForm(single(aliasOrIndex).mappings);
  }

  @Override
  public Map<String, Object> getSettings(String aliasOrIndex) {
    record("getSettings", aliasOrIndex);
    IndexState state = single(aliasOrIndex);
    Map<String, Object> index = new TreeMap<>();
    index.put("number_of_shards", "1");
    index.put("number_of_replicas", "1");
    index.putAll(state.settings);
    if (state.writeBlocked) {
      index.put("blocks", Map.of("write", "true"));
    }
    return Map.of("index", stringify(index));
  }

  @Override
  public void putMappings(String index, Map<String, Object> mappings) {
    record("putMappings", index);
    List<IndexState> targets = existing(index);
    for (IndexState state : targets) {
      if (rejectTypeChanges) {
        checkTypes("", properties(state.mappings), properties(mappings));
      }
    }
    for (IndexState state : targets) {
      Map<String, Object> merged = Json.deepMerge(state.mappings, mappings);
      if (mappings.get("_meta") instanceof Map) {
        merged.put("_meta", Json.sortedCopy(map(mappings.get("_meta"))));
      }
      state.mappings = merged;
    }
  }

  @Override
  public Map<String, Set<String>> getAliases(String aliasOrIndex) {
    record("getAliases", aliasOrIndex);
    Map<String, Set<String>> result = new TreeMap<>();
    for (String name : resolve(aliasOrIndex)) {
      result.put(name, new TreeSet<>(indices.get(name).aliases));
    }
    return result;
  }

  @Override
  public void updateAliases(List<AliasAction> actions) {
    List<String> described = new ArrayList<>();
    for (AliasAction action : actions) {
      described.add(action.type().name().toLowerCase(Locale.ROOT) + " "
          + String.join(",", action.indices())
          + (action.alias() == null ? "" : " " + action.alias()));
    }
    record("updateAliases", described.toString());

    Map<String, Set<String>> next = new TreeMap<>();
    indices.forEach((name, state) -> next.put(name, new TreeSet<>(state.aliases)));
    for (AliasAction action : actions) {
      for (String index : action.indices()) {
        Set<String> aliases = next.get(index);
        if (aliases == null) {
          throw notFound(index);
        }
        switch (action.type()) {
          case ADD:
            if (next.containsKey(action.alias())) {
              throw new EngineException(Kind.BAD_REQUEST, 400,
                  "invalid_alias_name_exception: an index exists with the same name as the alias ["
                      + action.alias() + "]");
            }
            aliases.add(action.alias());
            break;
          case REMOVE:
            if (!aliases.remove(action.alias())) {
              throw new EngineException(Kind.NOT_FOUND, 404,
                  "aliases_not_found_exception: aliases [" + action.alias() + "] missing on ["
                      + index + "]");
            }
            break;
          default:
            next.remove(index);
            break;
        }
      }
    }
    indices.keySet().retainAll(next.keySet());
    next.forEach((name, aliases) -> {
      IndexState state = indices.get(name);
      state.aliases.clear();
      state.aliases.addAll(aliases);
    });
    afterAliasUpdate.run();
  }

  /**
   * Merges settings into every index behind a name. Keys may be flat ({@code index.x.y}) or
   * nested under {@code index}; they are stored without the {@code index.} prefix, so {@link
   * #getSettings} keeps its shape. A null value removes a setting; {@code blocks.write} sets the
   * write block.
   */
  @Override
  public void putSettings(String index, Map<String, Object> settings) {
    record("putSettings", index + " " + new TreeMap<>(settings));
    List<IndexState> targets = existing(index);
    Map<String, Object> flat = flattenSettings(settings);
    Object block = flat.remove("blocks.write");
    if (flat.get("blocks") instanceof Map<?, ?> blocks && blocks.containsKey("write")) {
      block = blocks.get("write");
      Map<String, Object> rest = new TreeMap<>(map(blocks));
      rest.remove("write");
      if (rest.isEmpty()) {
        flat.remove("blocks");
      } else {
        flat.put("blocks", rest);
      }
    }
    for (IndexState state : targets) {
      flat.forEach((key, value) -> {
        if (value == null) {
          state.settings.remove(key);
        } else {
          state.settings.put(key, value);
        }
      });
      if (block != null) {
        state.writeBlocked = Boolean.parseBoolean(block.toString());
      }
    }
  }

  @Override
  public void setWriteBlock(String index, boolean blocked) {
    record("setWriteBlock", index + "=" + blocked);
    for (IndexState state : existing(index)) {
      state.writeBlocked = blocked;
    }
  }

  @Override
  public void refresh(String index) {
    record("refresh", index);
    existing(index);
  }

  @Override
  public ReindexResult reindex(ReindexRequest request) {
    record("reindex", request.source() + " -> " + request.dest());
    List<String> sources = resolve(request.source());
    if (sources.isEmpty()) {
      throw notFound(request.source());
    }
    IndexState dest = indices.get(request.dest());
    if (dest == null) {
      throw notFound(request.dest());
    }
    checkWritable(request.dest(), dest);
    List<String> statements = request.script() == null ? List.of() : parse(request.script());
    Map<String, Object> query = request.query() == null ? Map.of() : request.query();
    List<Snapshot> snapshot = new ArrayList<>();
    for (String source : sources) {
      IndexState state = indices.get(source);
      for (Map.Entry<String, Map<String, Object>> doc : state.docs.entrySet()) {
        if (matches(query, doc.getKey(), doc.getValue())) {
          snapshot.add(new Snapshot(doc.getKey(), Json.sortedCopy(doc.getValue()),
              state.versions.getOrDefault(doc.getKey(), 1L)));
        }
      }
    }
    duringReindex.accept(request);
    boolean external = request.versionType() == ReindexRequest.VersionType.EXTERNAL;
    long created = 0;
    long updated = 0;
    long conflicts = 0;
    List<Map<String, Object>> failures = new ArrayList<>();
    for (Snapshot doc : snapshot) {
      Long current = dest.versions.containsKey(doc.id())
          ? dest.versions.get(doc.id()) : tombstone(dest, doc.id());
      if (external && current != null && current >= doc.version()) {
        conflicts++;
        if (!request.proceedOnConflicts()) {
          failures.add(conflictFailure(request.dest(), doc.id(), "version conflict, current"
              + " version [" + current + "] is higher or equal to the one provided ["
              + doc.version() + "]"));
          break;
        }
        continue;
      }
      Map<String, Object> copy = doc.source();
      String id = apply(statements, copy, request.scriptParams(), doc.id());
      if (dest.docs.put(id, copy) == null) {
        created++;
      } else {
        updated++;
      }
      stamp(dest, id, external ? doc.version() : null);
    }
    return new ReindexResult(snapshot.size(), created, updated, failures, 0, false, 0, 0,
        conflicts);
  }

  /**
   * Rewrites every matching document in place, bumping its version and sequence number; a
   * document changed since the sequence numbers were taken is a version conflict.
   */
  @Override
  public ReindexResult updateByQuery(UpdateByQueryRequest request) {
    record("updateByQuery", request.index());
    List<Taken> snapshot = new ArrayList<>();
    for (String name : existingNames(request.index())) {
      IndexState state = indices.get(name);
      checkWritable(name, state);
      for (Map.Entry<String, Map<String, Object>> doc : state.docs.entrySet()) {
        if (matches(request.query(), doc.getKey(), doc.getValue())) {
          snapshot.add(new Taken(name, doc.getKey(), state.seqNos.get(doc.getKey())));
        }
      }
    }
    duringUpdateByQuery.accept(request);
    long updated = 0;
    long conflicts = 0;
    List<Map<String, Object>> failures = new ArrayList<>();
    for (Taken entry : snapshot) {
      String name = entry.index();
      String id = entry.id();
      IndexState state = indices.get(name);
      Long now = state == null ? null : state.seqNos.get(id);
      if (!Objects.equals(now, entry.seqNo())) {
        conflicts++;
        if (!request.proceedOnConflicts()) {
          failures.add(conflictFailure(name, id, "version conflict, required seqNo ["
              + entry.seqNo() + "], primary term [" + PRIMARY_TERM + "], "
              + (now == null ? "but no document was found" : "current document has seqNo [" + now
                  + "] and primary term [" + PRIMARY_TERM + "]")));
          break;
        }
        continue;
      }
      stamp(state, id, null);
      updated++;
    }
    return new ReindexResult(snapshot.size(), 0, updated, failures, 0, false, 0, 0, conflicts);
  }

  // ---------------------------------------------------------------- documents

  @Override
  public IndexResult index(
      String index, String id, Map<String, Object> document, IndexOptions options) {
    record("index", index + "/" + id);
    SearchEngine.checkConditionalId(id, options);
    String name = writeTarget(index);
    IndexState state = indices.get(name);
    checkWritable(name, state);
    String docId = id == null ? "generated-" + (++generatedIds) : id;
    if (options.createOnly() && state.docs.containsKey(docId)) {
      throw new EngineException(Kind.CONFLICT, 409,
          "version_conflict_engine_exception: [" + docId + "]: document already exists");
    }
    checkMatch(state, docId, options.ifSeqNo(), options.ifPrimaryTerm());
    boolean existed = state.docs.put(docId, Json.sortedCopy(document)) != null;
    long version = stamp(state, docId, null);
    return new IndexResult(name, docId, existed ? "updated" : "created", version,
        state.seqNos.get(docId), PRIMARY_TERM);
  }

  @Override
  public IndexResult update(String index, String id, Map<String, Object> partial) {
    record("update", index + "/" + id);
    String name = writeTarget(index);
    IndexState state = indices.get(name);
    checkWritable(name, state);
    Map<String, Object> current = state.docs.get(id);
    if (current == null) {
      throw new EngineException(Kind.NOT_FOUND, 404,
          "document_missing_exception: [" + id + "]: document missing");
    }
    state.docs.put(id, Json.deepMerge(current, partial));
    long version = stamp(state, id, null);
    return new IndexResult(name, id, "updated", version, state.seqNos.get(id), PRIMARY_TERM);
  }

  /**
   * Reads one document by id. Unknown indices and ids are empty, like a 404 from the engine; an
   * alias over several indices fails with {@code BAD_REQUEST}.
   */
  @Override
  public Optional<Hit> get(String index, String id) {
    record("get", index + "/" + id);
    List<String> names = resolve(index);
    if (names.size() > 1) {
      throw new EngineException(Kind.BAD_REQUEST, 400, "illegal_argument_exception: alias ["
          + index + "] has more than one index associated with it " + names
          + ", can't execute a single index op");
    }
    if (names.isEmpty()) {
      return Optional.empty();
    }
    String name = names.get(0);
    IndexState state = indices.get(name);
    Map<String, Object> doc = state.docs.get(id);
    return doc == null
        ? Optional.empty()
        : Optional.of(new Hit(name, id, null, Json.sortedCopy(doc), List.of(),
            state.versions.get(id), state.seqNos.get(id), PRIMARY_TERM, Map.of()));
  }

  /**
   * Deletes one document; the refresh option is accepted and ignored, writes are visible. A
   * conditional delete of a missing document fails with {@code CONFLICT}, like the engines.
   */
  @Override
  public boolean delete(String index, String id, IndexOptions options) {
    record("delete", index + "/" + id);
    String name = writeTarget(index);
    IndexState state = indices.get(name);
    checkWritable(name, state);
    checkMatch(state, id, options.ifSeqNo(), options.ifPrimaryTerm());
    return remove(state, id);
  }

  @Override
  public BulkResult bulk(List<BulkOp> operations) {
    record("bulk", Integer.toString(operations.size()));
    for (BulkOp op : operations) {
      String name = writeTarget(op.index());
      checkWritable(name, indices.get(name));
    }
    List<BulkResult.Item> items = new ArrayList<>();
    boolean errors = false;
    for (BulkOp op : operations) {
      String name = writeTarget(op.index());
      IndexState state = indices.get(name);
      String opName = op.type().name().toLowerCase(Locale.ROOT);
      String id = op.id() == null ? "generated-" + (++generatedIds) : op.id();
      boolean exists = state.docs.containsKey(id);
      int status;
      String error = null;
      String mismatch = op.ifSeqNo() == null ? null
          : mismatch(state, id, op.ifSeqNo(), op.ifPrimaryTerm());
      if (mismatch != null) {
        errors = true;
        items.add(new BulkResult.Item(opName, name, id, 409, mismatch));
        continue;
      }
      switch (op.type()) {
        case CREATE:
          if (exists) {
            status = 409;
            error = "version_conflict_engine_exception: document already exists";
          } else {
            state.docs.put(id, Json.sortedCopy(op.source()));
            stamp(state, id, null);
            status = 201;
          }
          break;
        case UPDATE:
          if (exists) {
            state.docs.put(id, Json.deepMerge(state.docs.get(id), op.source()));
            stamp(state, id, null);
            status = 200;
          } else {
            status = 404;
            error = "document_missing_exception: document missing";
          }
          break;
        case DELETE:
          remove(state, id);
          status = exists ? 200 : 404;
          break;
        default:
          state.docs.put(id, Json.sortedCopy(op.source()));
          stamp(state, id, null);
          status = exists ? 200 : 201;
          break;
      }
      errors |= error != null;
      items.add(new BulkResult.Item(opName, name, id, status, error));
    }
    return new BulkResult(errors, items, 0);
  }

  // ---------------------------------------------------------------- search

  @Override
  public SearchResult search(List<String> targets, Map<String, Object> body) {
    Map<String, Object> pit = map(body.get("pit"));
    List<String> names = new ArrayList<>();
    String pitId = null;
    Map<String, IndexState> snapshot = null;
    if (!pit.isEmpty()) {
      pitId = String.valueOf(pit.get("id"));
      record("search", "pit=" + pitId);
      snapshot = pits.get(pitId);
      if (snapshot == null) {
        throw new EngineException(Kind.NOT_FOUND, 404,
            "search_context_missing_exception: no point in time " + pitId);
      }
      names.addAll(snapshot.keySet());
    } else {
      record("search", String.join(",", targets));
      Set<String> unique = new LinkedHashSet<>();
      for (String target : targets) {
        unique.addAll(existingNames(target));
      }
      names.addAll(unique);
    }

    Map<String, Object> query = map(body.get("query"));
    boolean withVersion = Boolean.TRUE.equals(body.get("version"));
    boolean withSeqNo = Boolean.TRUE.equals(body.get("seq_no_primary_term"));
    List<Hit> matches = new ArrayList<>();
    Map<Hit, Long> seqNoOf = new java.util.IdentityHashMap<>();
    for (String name : names) {
      IndexState state = snapshot != null ? snapshot.get(name) : indices.get(name);
      if (state == null) {
        continue;
      }
      for (Map.Entry<String, Map<String, Object>> doc : state.docs.entrySet()) {
        CURRENT_SEQ_NO.set(state.seqNos.getOrDefault(doc.getKey(), -1L));
        boolean hit;
        try {
          hit = matches(query, doc.getKey(), doc.getValue());
        } finally {
          CURRENT_SEQ_NO.remove();
        }
        if (hit) {
          String id = doc.getKey();
          Hit found = new Hit(name, id, 1.0, Json.sortedCopy(doc.getValue()), List.of(),
              withVersion ? state.versions.get(id) : null,
              withSeqNo ? state.seqNos.get(id) : null,
              withSeqNo ? PRIMARY_TERM : null,
              Map.of());
          matches.add(found);
          seqNoOf.put(found, state.seqNos.getOrDefault(id, -1L));
        }
      }
    }
    List<Object> order = list(body.get("sort"));
    if (!order.isEmpty() && map(order.get(0)).containsKey("_seq_no")) {
      // a stable sort keeps insertion order among equal sequence numbers
      matches.sort(java.util.Comparator.comparingLong(seqNoOf::get));
    }
    if (body.containsKey("sort")) {
      for (int i = 0; i < matches.size(); i++) {
        Hit hit = matches.get(i);
        matches.set(i, new Hit(hit.index(), hit.id(), hit.score(), hit.source(),
            List.of((long) i), hit.version(), hit.seqNo(), hit.primaryTerm(), hit.highlight()));
      }
    }
    int from = intValue(body.get("from"), 0);
    List<Object> searchAfter = list(body.get("search_after"));
    if (!searchAfter.isEmpty()) {
      from = intValue(searchAfter.get(0), -1) + 1;
    }
    int size = intValue(body.get("size"), 10);
    List<Hit> page = matches.subList(Math.min(from, matches.size()),
        Math.min(from + size, matches.size()));
    Map<String, Object> aggs = map(body.containsKey("aggs") ? body.get("aggs")
        : body.get("aggregations"));
    Map<String, Object> aggregations = Map.of();
    if (!aggs.isEmpty()) {
      List<InMemoryAggregations.Scope> roots = new ArrayList<>();
      for (Hit hit : matches) {
        roots.add(new InMemoryAggregations.Scope(hit.source(), "", null));
      }
      Map<String, String> types = new TreeMap<>();
      for (String name : names) {
        IndexState state = indices.get(name);
        if (state != null) {
          types.putAll(InMemoryAggregations.fieldTypes(state.mappings));
        }
      }
      aggregations = new InMemoryAggregations(types).run(aggs, roots);
    }
    // like the real engines, track_total_hits: false leaves the total out
    boolean counted = !Boolean.FALSE.equals(body.get("track_total_hits"));
    return new SearchResult(counted ? matches.size() : SearchResult.NO_TOTAL, counted,
        new ArrayList<>(page), pitId, aggregations, 0);
  }

  @Override
  public Pit openPit(List<String> targets, Duration keepAlive) {
    record("openPit", String.join(",", targets));
    Set<String> names = new LinkedHashSet<>();
    for (String target : targets) {
      names.addAll(existingNames(target));
    }
    String id = UUID.randomUUID().toString();
    Map<String, IndexState> snapshot = new LinkedHashMap<>();
    for (String name : names) {
      IndexState live = indices.get(name);
      IndexState copy = new IndexState();
      live.docs.forEach((docId, source) -> copy.docs.put(docId, Json.sortedCopy(source)));
      copy.versions.putAll(live.versions);
      copy.seqNos.putAll(live.seqNos);
      snapshot.put(name, copy);
    }
    pits.put(id, snapshot);
    return new Pit(id, keepAlive);
  }

  @Override
  public void closePit(Pit pit) {
    record("closePit", pit.id());
    pits.remove(pit.id());
  }

  // ---------------------------------------------------------------- internals

  /**
   * Records a write: takes the next sequence number and sets the version, to {@code version} when
   * given (external versioning), else one more than before.
   */
  private long stamp(IndexState state, String id, Long version) {
    Long previous = state.versions.containsKey(id)
        ? state.versions.get(id) : tombstone(state, id);
    long next = version != null ? version : (previous == null ? 0L : previous) + 1;
    state.tombstones.remove(id);
    state.tombstoneTimes.remove(id);
    state.versions.put(id, next);
    state.seqNos.put(id, ++seqNo);
    return next;
  }

  /** Returns the tombstone version of an id, dropping it once {@link #gcDeletes} has passed. */
  private Long tombstone(IndexState state, String id) {
    Long time = state.tombstoneTimes.get(id);
    if (gcDeletes != null && time != null && System.nanoTime() - time >= gcDeletes.toNanos()) {
      state.tombstones.remove(id);
      state.tombstoneTimes.remove(id);
    }
    return state.tombstones.get(id);
  }

  /** Deletes a document, leaving its bumped version behind as a tombstone. */
  private boolean remove(IndexState state, String id) {
    Long version = state.versions.remove(id);
    if (version != null) {
      state.tombstones.put(id, version + 1);
      state.tombstoneTimes.put(id, System.nanoTime());
    }
    state.seqNos.remove(id);
    seqNo++;
    return state.docs.remove(id) != null;
  }

  /** Fails with {@code CONFLICT} when a conditional write does not match the stored document. */
  private static void checkMatch(IndexState state, String id, Long ifSeqNo, Long ifPrimaryTerm) {
    String mismatch = ifSeqNo == null ? null : mismatch(state, id, ifSeqNo, ifPrimaryTerm);
    if (mismatch != null) {
      throw new EngineException(Kind.CONFLICT, 409, mismatch);
    }
  }

  /** Describes how a conditional write fails to match, or returns null when it matches. */
  private static String mismatch(IndexState state, String id, long ifSeqNo, long ifPrimaryTerm) {
    Long current = state.seqNos.get(id);
    String required = "version_conflict_engine_exception: [" + id + "]: version conflict,"
        + " required seqNo [" + ifSeqNo + "], primary term [" + ifPrimaryTerm + "]";
    if (current == null) {
      return required + " but no document was found";
    }
    if (current != ifSeqNo || ifPrimaryTerm != PRIMARY_TERM) {
      return required + ". current document has seqNo [" + current + "] and primary term ["
          + PRIMARY_TERM + "]";
    }
    return null;
  }

  private static Map<String, Object> conflictFailure(String index, String id, String reason) {
    Map<String, Object> cause = new TreeMap<>();
    cause.put("type", "version_conflict_engine_exception");
    cause.put("reason", "[" + id + "]: " + reason);
    Map<String, Object> failure = new TreeMap<>();
    failure.put("index", index);
    failure.put("id", id);
    failure.put("status", 409);
    failure.put("cause", cause);
    return failure;
  }

  private void record(String operation, String target) {
    log.add(target.isEmpty() ? operation : operation + " " + target);
  }

  private boolean isAlias(String name) {
    return indices.values().stream().anyMatch(state -> state.aliases.contains(name));
  }

  private List<String> resolve(String name) {
    if (indices.containsKey(name)) {
      return List.of(name);
    }
    List<String> result = new ArrayList<>();
    indices.forEach((index, state) -> {
      if (state.aliases.contains(name)) {
        result.add(index);
      }
    });
    return result;
  }

  private List<String> existingNames(String name) {
    List<String> names = resolve(name);
    if (names.isEmpty()) {
      throw notFound(name);
    }
    return names;
  }

  private List<IndexState> existing(String name) {
    List<IndexState> states = new ArrayList<>();
    for (String index : existingNames(name)) {
      states.add(indices.get(index));
    }
    return states;
  }

  private IndexState single(String name) {
    List<String> names = existingNames(name);
    if (names.size() > 1) {
      throw new EngineException(Kind.CONFLICT, 0,
          name + " resolves to " + names.size() + " indices: " + names);
    }
    return indices.get(names.get(0));
  }

  private IndexState concrete(String index) {
    IndexState state = indices.get(index);
    if (state == null) {
      throw notFound(index);
    }
    return state;
  }

  private String writeTarget(String name) {
    List<String> names = existingNames(name);
    if (names.size() > 1) {
      throw new EngineException(Kind.BAD_REQUEST, 400,
          "illegal_argument_exception: alias [" + name + "] has more than one index and no write"
              + " index");
    }
    return names.get(0);
  }

  private static void checkWritable(String name, IndexState state) {
    if (state.writeBlocked) {
      throw new EngineException(Kind.BLOCKED, 403,
          "cluster_block_exception: index [" + name + "] blocked by: [FORBIDDEN/8/index write"
              + " (api)] (blocked: lift the block before retrying)");
    }
  }

  private static EngineException notFound(String name) {
    return new EngineException(Kind.NOT_FOUND, 404,
        "index_not_found_exception: no such index [" + name + "]");
  }

  private void checkTypes(
      String prefix, Map<String, Object> existing, Map<String, Object> desired) {
    for (Map.Entry<String, Object> entry : desired.entrySet()) {
      Map<String, Object> before = map(existing.get(entry.getKey()));
      Map<String, Object> after = map(entry.getValue());
      if (before.isEmpty()) {
        continue;
      }
      String path = prefix + entry.getKey();
      String beforeType = typeOf(before);
      String afterType = typeOf(after);
      if (!beforeType.equals(afterType)) {
        throw new EngineException(Kind.BAD_REQUEST, 400,
            "illegal_argument_exception: mapper [" + path + "] cannot be changed from type ["
                + beforeType + "] to [" + afterType + "]");
      }
      checkTypes(path + ".", properties(before), properties(after));
    }
  }

  private static String typeOf(Map<String, Object> property) {
    Object type = property.get("type");
    return type == null ? "object" : type.toString();
  }

  private static Map<String, Object> properties(Map<String, Object> mapping) {
    return map(mapping.get("properties"));
  }

  /** Renders stored mappings like the engine does: string dynamic, no {@code type: object}. */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> engineForm(Map<String, Object> mapping) {
    Map<String, Object> copy = new TreeMap<>();
    for (Map.Entry<String, Object> entry : mapping.entrySet()) {
      Object value = entry.getValue();
      if ("dynamic".equals(entry.getKey()) && value != null) {
        value = value.toString();
      } else if ("properties".equals(entry.getKey()) && value instanceof Map) {
        Map<String, Object> props = new TreeMap<>();
        ((Map<String, Object>) value).forEach((name, property) ->
            props.put(name, property instanceof Map
                ? engineForm((Map<String, Object>) property) : property));
        value = props;
      } else if (value instanceof Map) {
        value = Json.sortedCopy((Map<String, Object>) value);
      }
      copy.put(entry.getKey(), value);
    }
    if ("object".equals(copy.get("type")) && copy.containsKey("properties")) {
      copy.remove("type");
    }
    return copy;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> flattenSettings(Map<String, Object> settings) {
    Map<String, Object> flat = new TreeMap<>();
    for (Map.Entry<String, Object> entry : settings.entrySet()) {
      String key = entry.getKey();
      if ("index".equals(key) && entry.getValue() instanceof Map) {
        flat.putAll(flattenSettings((Map<String, Object>) entry.getValue()));
      } else {
        flat.put(key.startsWith("index.") ? key.substring("index.".length()) : key,
            entry.getValue() instanceof Map
                ? Json.sortedCopy((Map<String, Object>) entry.getValue()) : entry.getValue());
      }
    }
    return flat;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> stringify(Map<String, Object> value) {
    Map<String, Object> copy = new TreeMap<>();
    value.forEach((key, item) -> copy.put(key, stringifyValue(item)));
    return copy;
  }

  @SuppressWarnings("unchecked")
  private static Object stringifyValue(Object value) {
    if (value instanceof Map) {
      return stringify((Map<String, Object>) value);
    }
    if (value instanceof List) {
      List<Object> copy = new ArrayList<>();
      for (Object item : (List<Object>) value) {
        copy.add(stringifyValue(item));
      }
      return copy;
    }
    return value == null ? null : value.toString();
  }

  private static List<String> parse(String script) {
    List<String> statements = new ArrayList<>();
    for (String line : script.split("\\R")) {
      String statement = line.trim();
      if (statement.isEmpty() || statement.startsWith("//")) {
        continue;
      }
      if (!ASSIGN.matcher(statement).matches() && !REMOVE.matcher(statement).matches()
          && !ASSIGN_ID.matcher(statement).matches()) {
        throw new EngineException(Kind.OTHER, 400,
            "script_exception: the in-memory engine does not support: " + statement);
      }
      statements.add(statement);
    }
    return statements;
  }

  /** Runs the statements on a document and returns its id, which a statement may change. */
  private static String apply(List<String> statements, Map<String, Object> source,
      Map<String, Object> params, String id) {
    String result = id;
    for (String statement : statements) {
      Matcher assign = ASSIGN.matcher(statement);
      Matcher assignId = ASSIGN_ID.matcher(statement);
      if (assign.matches()) {
        source.put(assign.group(1), params.get(assign.group(2)));
      } else if (assignId.matches()) {
        result = String.valueOf(params.get(assignId.group(1)));
      } else {
        Matcher remove = REMOVE.matcher(statement);
        remove.matches();
        source.remove(remove.group(1));
      }
    }
    return result;
  }

  private static boolean matches(Map<String, Object> query, String id,
      Map<String, Object> source) {
    if (query.isEmpty() || query.containsKey("match_all")) {
      return true;
    }
    if (query.size() != 1) {
      throw unsupported(query);
    }
    if (query.containsKey("bool")) {
      return matchesBool(map(query.get("bool")), id, source);
    }
    if (query.containsKey("ids")) {
      return list(map(query.get("ids")).get("values")).stream()
          .anyMatch(value -> String.valueOf(value).equals(id));
    }
    if (query.containsKey("range")) {
      java.util.function.LongPredicate range = seqNoRange(query);
      Long seqNo = CURRENT_SEQ_NO.get();
      if (range == null || map(query.get("range")).size() != 1 || seqNo == null) {
        throw unsupported(query);
      }
      return range.test(seqNo);
    }
    boolean terms = query.containsKey("terms");
    Map<String, Object> term = map(query.get(terms ? "terms" : "term"));
    if (term.size() != 1) {
      throw unsupported(query);
    }
    Map.Entry<String, Object> condition = term.entrySet().iterator().next();
    List<Object> expected;
    if (terms) {
      expected = list(condition.getValue());
    } else {
      expected = List.of(condition.getValue() instanceof Map
          ? map(condition.getValue()).get("value") : condition.getValue());
    }
    List<Object> actual = new ArrayList<>();
    collect(source, condition.getKey().split("\\."), 0, actual);
    return actual.stream().anyMatch(item -> expected.stream()
        .anyMatch(value -> sameValue(item, value)));
  }

  private static boolean matchesBool(Map<String, Object> bool, String id,
      Map<String, Object> source) {
    for (String key : bool.keySet()) {
      if (!Set.of("filter", "must", "must_not", "should", "minimum_should_match").contains(key)) {
        throw unsupported(Map.of("bool", bool));
      }
    }
    for (String key : List.of("filter", "must")) {
      for (Object clause : clauses(bool.get(key))) {
        if (!matches(map(clause), id, source)) {
          return false;
        }
      }
    }
    for (Object clause : clauses(bool.get("must_not"))) {
      if (matches(map(clause), id, source)) {
        return false;
      }
    }
    List<Object> should = clauses(bool.get("should"));
    boolean scoringOnly = clauses(bool.get("filter")).isEmpty()
        && clauses(bool.get("must")).isEmpty();
    int minimum = intValue(bool.get("minimum_should_match"), scoringOnly ? 1 : 0);
    if (should.isEmpty()) {
      return true;
    }
    long matched = should.stream().filter(clause -> matches(map(clause), id, source)).count();
    return matched >= minimum;
  }

  /** Returns {@code gt} of a top-level {@code {"range": {"_seq_no": {"gt": n}}}} query, or null. */
  private static java.util.function.LongPredicate seqNoRange(Map<String, Object> query) {
    Map<String, Object> range = map(map(query.get("range")).get("_seq_no"));
    if (range.isEmpty()) {
      return null;
    }
    java.util.function.LongPredicate test = seqNo -> true;
    for (Map.Entry<String, Object> bound : range.entrySet()) {
      long value = ((Number) bound.getValue()).longValue();
      switch (bound.getKey()) {
        case "gt":
          test = test.and(seqNo -> seqNo > value);
          break;
        case "gte":
          test = test.and(seqNo -> seqNo >= value);
          break;
        case "lt":
          test = test.and(seqNo -> seqNo < value);
          break;
        case "lte":
          test = test.and(seqNo -> seqNo <= value);
          break;
        default:
          throw new EngineException(Kind.BAD_REQUEST, 400,
              "unsupported _seq_no range bound " + bound.getKey());
      }
    }
    return test;
  }

  /** Collects the values at a dotted path, walking into arrays. */
  private static void collect(Object node, String[] path, int depth, List<Object> out) {
    if (node instanceof List) {
      for (Object item : (List<?>) node) {
        collect(item, path, depth, out);
      }
    } else if (depth == path.length) {
      if (node != null) {
        out.add(node);
      }
    } else if (node instanceof Map) {
      collect(((Map<?, ?>) node).get(path[depth]), path, depth + 1, out);
    }
  }

  private static List<Object> clauses(Object value) {
    if (value == null) {
      return List.of();
    }
    return value instanceof List ? list(value) : List.of(value);
  }

  @SuppressWarnings("unchecked")
  private static List<Object> list(Object value) {
    return value instanceof List ? (List<Object>) value : List.of();
  }

  private static EngineException unsupported(Map<String, Object> query) {
    return new EngineException(Kind.BAD_REQUEST, 400,
        "the in-memory engine supports only match_all, term, terms, ids and bool, not " + query);
  }

  private static boolean sameValue(Object actual, Object expected) {
    return actual != null && expected != null && actual.toString().equals(expected.toString());
  }

  private static int intValue(Object value, int fallback) {
    return value instanceof Number ? ((Number) value).intValue() : fallback;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : Map.of();
  }

  /** One concrete index. */
  private static final class IndexState {
    private Map<String, Object> settings = new TreeMap<>();
    private Map<String, Object> mappings = new TreeMap<>();
    private final Set<String> aliases = new TreeSet<>();
    private final Map<String, Map<String, Object>> docs = new LinkedHashMap<>();
    private final Map<String, Long> versions = new TreeMap<>();
    private final Map<String, Long> seqNos = new TreeMap<>();
    private final Map<String, Long> tombstones = new TreeMap<>();
    private final Map<String, Long> tombstoneTimes = new TreeMap<>();
    private boolean writeBlocked;
  }

  /** A source document as a reindex saw it when it started. */
  private record Snapshot(String id, Map<String, Object> source, long version) {}

  /** A document an update by query matched, with its sequence number at that time. */
  private record Taken(String index, String id, Long seqNo) {}
}
