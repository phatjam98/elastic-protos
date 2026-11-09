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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
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
 *   <li>{@link #putMappings} deep-merges {@code properties} and {@code _meta}; with {@link
 *       #rejectTypeChanges(boolean)} (on by default) it refuses to change an existing field's type.
 *   <li>{@link #updateAliases} validates every action before applying any.
 *   <li>Writes to a write-blocked index fail with {@code BLOCKED} and status 403.
 * </ul>
 *
 * <p>Limits: documents are never validated against mappings; every write is immediately visible
 * (refresh is a no-op); writes to unknown indices fail instead of auto-creating them; {@link
 * #reindex} requires the destination to exist and supports only a tiny stand-in for Painless
 * (one statement per line, {@code ;} optional, blank and {@code //} lines ignored):
 * {@code ctx._source.<field> = params.<name>} and {@code ctx._source.remove('<field>')};
 * {@link #search} supports only the queries {@code match_all}, {@code term} and {@code terms} on
 * one field (dotted paths walk into objects and arrays), {@code ids}, and {@code bool} with
 * {@code filter}, {@code must}, {@code must_not} and {@code should} (with {@code
 * minimum_should_match}, default 1 when there is no filter or must clause, else 0) over those, plus
 * {@code from}, {@code size}, {@code track_total_hits}, {@code pit} and {@code search_after}. Hits
 * always come in insertion order; when the body has a {@code sort}, whatever it says, each hit's
 * only sort value is its position among the matches, and {@code search_after} resumes after that
 * position, which is enough for point-in-time paging. Anything else fails with {@code
 * BAD_REQUEST}.
 *
 * <p>Every call is recorded in {@link #log()} as the operation name and its target, for example
 * {@code setWriteBlock documents-v1=true}, so specs can assert sequences.
 */
public class InMemorySearchEngine implements SearchEngine {

  private static final Pattern ASSIGN =
      Pattern.compile("ctx\\._source\\.([A-Za-z0-9_]+)\\s*=\\s*params\\.([A-Za-z0-9_]+)\\s*;?");
  private static final Pattern REMOVE =
      Pattern.compile("ctx\\._source\\.remove\\(\\s*'([^']+)'\\s*\\)\\s*;?");

  private final Map<String, IndexState> indices = new TreeMap<>();
  private final Map<String, List<String>> pits = new LinkedHashMap<>();
  private final List<String> log = new ArrayList<>();
  private EngineInfo info = new EngineInfo(EngineProduct.ELASTICSEARCH, "9.0.0", "in-memory");
  private int dataNodes = 1;
  private boolean rejectTypeChanges = true;
  private long generatedIds;

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
      state.mappings = Json.deepMerge(state.mappings, mappings);
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
    long created = 0;
    long updated = 0;
    long total = 0;
    for (String source : sources) {
      for (Map.Entry<String, Map<String, Object>> doc : indices.get(source).docs.entrySet()) {
        Map<String, Object> copy = Json.sortedCopy(doc.getValue());
        apply(statements, copy, request.scriptParams());
        total++;
        if (dest.docs.put(doc.getKey(), copy) == null) {
          created++;
        } else {
          updated++;
        }
        dest.bumpVersion(doc.getKey());
      }
    }
    return new ReindexResult(total, created, updated, List.of(), 0, false);
  }

  // ---------------------------------------------------------------- documents

  @Override
  public IndexResult index(
      String index, String id, Map<String, Object> document, IndexOptions options) {
    record("index", index + "/" + id);
    String name = writeTarget(index);
    IndexState state = indices.get(name);
    checkWritable(name, state);
    String docId = id == null ? "generated-" + (++generatedIds) : id;
    if (options.createOnly() && state.docs.containsKey(docId)) {
      throw new EngineException(Kind.CONFLICT, 409,
          "version_conflict_engine_exception: [" + docId + "]: document already exists");
    }
    boolean existed = state.docs.put(docId, Json.sortedCopy(document)) != null;
    return new IndexResult(name, docId, existed ? "updated" : "created", state.bumpVersion(docId));
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
    return new IndexResult(name, id, "updated", state.bumpVersion(id));
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
    Map<String, Object> doc = indices.get(name).docs.get(id);
    return doc == null
        ? Optional.empty()
        : Optional.of(new Hit(name, id, null, Json.sortedCopy(doc), List.of()));
  }

  /** Deletes one document; the refresh option is accepted and ignored, writes are visible. */
  @Override
  public boolean delete(String index, String id, IndexOptions options) {
    record("delete", index + "/" + id);
    String name = writeTarget(index);
    IndexState state = indices.get(name);
    checkWritable(name, state);
    state.versions.remove(id);
    return state.docs.remove(id) != null;
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
      switch (op.type()) {
        case CREATE:
          if (exists) {
            status = 409;
            error = "version_conflict_engine_exception: document already exists";
          } else {
            state.docs.put(id, Json.sortedCopy(op.source()));
            state.bumpVersion(id);
            status = 201;
          }
          break;
        case UPDATE:
          if (exists) {
            state.docs.put(id, Json.deepMerge(state.docs.get(id), op.source()));
            state.bumpVersion(id);
            status = 200;
          } else {
            status = 404;
            error = "document_missing_exception: document missing";
          }
          break;
        case DELETE:
          state.docs.remove(id);
          state.versions.remove(id);
          status = exists ? 200 : 404;
          break;
        default:
          state.docs.put(id, Json.sortedCopy(op.source()));
          state.bumpVersion(id);
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
    if (!pit.isEmpty()) {
      pitId = String.valueOf(pit.get("id"));
      record("search", "pit=" + pitId);
      List<String> pitIndices = pits.get(pitId);
      if (pitIndices == null) {
        throw new EngineException(Kind.NOT_FOUND, 404,
            "search_context_missing_exception: no point in time " + pitId);
      }
      names.addAll(pitIndices);
    } else {
      record("search", String.join(",", targets));
      Set<String> unique = new LinkedHashSet<>();
      for (String target : targets) {
        unique.addAll(existingNames(target));
      }
      names.addAll(unique);
    }

    Map<String, Object> query = map(body.get("query"));
    List<Hit> matches = new ArrayList<>();
    for (String name : names) {
      IndexState state = indices.get(name);
      if (state == null) {
        continue;
      }
      for (Map.Entry<String, Map<String, Object>> doc : state.docs.entrySet()) {
        if (matches(query, doc.getKey(), doc.getValue())) {
          matches.add(new Hit(name, doc.getKey(), 1.0, Json.sortedCopy(doc.getValue()),
              List.of()));
        }
      }
    }
    if (body.containsKey("sort")) {
      for (int i = 0; i < matches.size(); i++) {
        Hit hit = matches.get(i);
        matches.set(i, new Hit(hit.index(), hit.id(), hit.score(), hit.source(),
            List.of((long) i)));
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
    boolean exact = !Boolean.FALSE.equals(body.get("track_total_hits"));
    return new SearchResult(exact ? matches.size() : page.size(), exact, new ArrayList<>(page),
        pitId, Map.of(), 0);
  }

  @Override
  public Pit openPit(List<String> targets, Duration keepAlive) {
    record("openPit", String.join(",", targets));
    Set<String> names = new LinkedHashSet<>();
    for (String target : targets) {
      names.addAll(existingNames(target));
    }
    String id = UUID.randomUUID().toString();
    pits.put(id, new ArrayList<>(names));
    return new Pit(id, keepAlive);
  }

  @Override
  public void closePit(Pit pit) {
    record("closePit", pit.id());
    pits.remove(pit.id());
  }

  // ---------------------------------------------------------------- internals

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
              + " (api)] (the index or cluster is write-blocked)");
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
      if (!ASSIGN.matcher(statement).matches() && !REMOVE.matcher(statement).matches()) {
        throw new EngineException(Kind.OTHER, 400,
            "script_exception: the in-memory engine does not support: " + statement);
      }
      statements.add(statement);
    }
    return statements;
  }

  private static void apply(
      List<String> statements, Map<String, Object> source, Map<String, Object> params) {
    for (String statement : statements) {
      Matcher assign = ASSIGN.matcher(statement);
      if (assign.matches()) {
        source.put(assign.group(1), params.get(assign.group(2)));
      } else {
        Matcher remove = REMOVE.matcher(statement);
        remove.matches();
        source.remove(remove.group(1));
      }
    }
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
    private boolean writeBlocked;

    long bumpVersion(String id) {
      return versions.merge(id, 1L, Long::sum);
    }
  }
}
