package com.phatjam98.elasticprotos.engine.rest;

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
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * A complete {@link SearchEngine} over the REST API shared by Elasticsearch and OpenSearch. The
 * two differ in how a point in time is opened and closed, and in the name of the flattened field
 * type ({@code flattened} on Elasticsearch, {@code flat_object} on OpenSearch 2.7+); both are
 * chosen from the product reported by the cluster. Mappings always cross this class in the
 * Elasticsearch spelling, so the mapper, its hashes and the migrator's diffs are the same on both
 * engines. Adapters supply the {@link RawHttp} transport.
 */
public final class JsonRestEngine implements SearchEngine {

  private static final Duration DEFAULT_POLL = Duration.ofMillis(500);
  private static final String FLATTENED = "flattened";
  private static final String FLAT_OBJECT = "flat_object";

  private final RawHttp http;
  private final Duration reindexPoll;
  private volatile EngineInfo info;

  /** Creates an engine over a transport, polling reindex tasks every half second. */
  public JsonRestEngine(RawHttp http) {
    this(http, DEFAULT_POLL);
  }

  /** Creates an engine over a transport with a custom reindex poll interval. */
  public JsonRestEngine(RawHttp http, Duration reindexPoll) {
    this.http = http;
    this.reindexPoll = reindexPoll;
  }

  /**
   * Returns the interval between polls of a running reindex task.
   *
   * @return the reindex poll interval
   */
  public Duration reindexPollInterval() {
    return reindexPoll;
  }

  @Override
  public EngineInfo info() {
    EngineInfo cached = info;
    if (cached == null) {
      Map<String, Object> root = object(require(getRequest("/", Map.of())));
      Map<String, Object> version = map(root.get("version"));
      EngineProduct product =
          "opensearch".equals(version.get("distribution"))
              ? EngineProduct.OPENSEARCH
              : (version.containsKey("number") ? EngineProduct.ELASTICSEARCH
                  : EngineProduct.UNKNOWN);
      cached = new EngineInfo(product, string(version.get("number")),
          string(root.get("cluster_name")));
      info = cached;
    }
    return cached;
  }

  @Override
  public boolean indexExists(String aliasOrIndex) {
    RawHttp.Response response = perform("HEAD", "/" + aliasOrIndex, Map.of(), null, RawHttp.JSON);
    if (response.status() == 404) {
      return false;
    }
    require(response);
    return true;
  }

  @Override
  public void createIndex(
      String name, Map<String, Object> settings, Map<String, Object> mappings,
      Set<String> aliases) {
    Map<String, Object> body = new TreeMap<>();
    if (!settings.isEmpty()) {
      body.put("settings", settings);
    }
    body.put("mappings", toEngine(mappings));
    if (!aliases.isEmpty()) {
      Map<String, Object> aliasBody = new TreeMap<>();
      for (String alias : aliases) {
        aliasBody.put(alias, Map.of());
      }
      body.put("aliases", aliasBody);
    }
    require(perform("PUT", "/" + name, Map.of(), Json.canonical(body), RawHttp.JSON));
  }

  @Override
  public void deleteIndex(String index) {
    require(perform("DELETE", "/" + index, Map.of(), null, RawHttp.JSON));
  }

  @Override
  public Map<String, Object> getMappings(String aliasOrIndex) {
    return renameType(map(single(aliasOrIndex, "/_mapping").get("mappings")), FLAT_OBJECT,
        FLATTENED);
  }

  @Override
  public Map<String, Object> getSettings(String aliasOrIndex) {
    return map(single(aliasOrIndex, "/_settings").get("settings"));
  }

  @Override
  public void putMappings(String index, Map<String, Object> mappings) {
    require(perform("PUT", "/" + index + "/_mapping", Map.of(),
        Json.canonical(toEngine(mappings)), RawHttp.JSON));
  }

  @Override
  public Map<String, Set<String>> getAliases(String aliasOrIndex) {
    RawHttp.Response response = getRequest("/" + aliasOrIndex + "/_alias", Map.of());
    if (response.status() == 404) {
      return Map.of();
    }
    Map<String, Set<String>> result = new TreeMap<>();
    for (Map.Entry<String, Object> entry : object(require(response)).entrySet()) {
      result.put(entry.getKey(), new TreeSet<>(map(map(entry.getValue()).get("aliases")).keySet()));
    }
    return result;
  }

  @Override
  public void updateAliases(List<AliasAction> actions) {
    List<Object> body = new ArrayList<>();
    for (AliasAction action : actions) {
      Map<String, Object> spec = new TreeMap<>();
      spec.put("indices", action.indices());
      if (action.alias() != null) {
        spec.put("alias", action.alias());
      }
      String verb = action.type() == AliasAction.Type.ADD ? "add"
          : action.type() == AliasAction.Type.REMOVE ? "remove" : "remove_index";
      body.add(Map.of(verb, spec));
    }
    require(perform("POST", "/_aliases", Map.of(), Json.canonical(Map.of("actions", body)),
        RawHttp.JSON));
  }

  @Override
  public ReindexResult reindex(ReindexRequest request) {
    Map<String, Object> body = new TreeMap<>();
    body.put("source", Map.of("index", request.source()));
    body.put("dest", Map.of("index", request.dest()));
    if (request.script() != null) {
      Map<String, Object> script = new TreeMap<>();
      script.put("lang", "painless");
      script.put("source", request.script());
      if (!request.scriptParams().isEmpty()) {
        script.put("params", request.scriptParams());
      }
      body.put("script", script);
    }
    Map<String, String> params = new TreeMap<>();
    params.put("wait_for_completion", "false");
    params.put("refresh", Boolean.toString(request.refresh()));
    Map<String, Object> started =
        object(require(perform("POST", "/_reindex", params, Json.canonical(body), RawHttp.JSON)));
    String task = string(started.get("task"));

    long deadline = System.nanoTime() + request.timeout().toNanos();
    while (true) {
      Map<String, Object> status = object(require(getRequest("/_tasks/" + task, Map.of())));
      if (Boolean.TRUE.equals(status.get("completed"))) {
        Map<String, Object> response = map(status.get("response"));
        Map<String, Object> error = map(status.get("error"));
        if (!error.isEmpty()) {
          throw new EngineException(Kind.OTHER, 0,
              "reindex task " + task + " failed: " + error.get("reason"));
        }
        return new ReindexResult(
            number(response.get("total")),
            number(response.get("created")),
            number(response.get("updated")),
            list(response.get("failures")),
            number(response.get("took")),
            Boolean.TRUE.equals(response.get("timed_out")));
      }
      if (System.nanoTime() > deadline) {
        throw new EngineException(Kind.TIMEOUT, 0,
            "reindex task " + task + " did not finish within " + request.timeout());
      }
      sleep();
    }
  }

  @Override
  public void setWriteBlock(String index, boolean blocked) {
    require(perform("PUT", "/" + index + "/_settings", Map.of(),
        Json.canonical(Map.of("index.blocks.write", blocked)), RawHttp.JSON));
  }

  @Override
  public IndexResult index(
      String index, String id, Map<String, Object> document, IndexOptions options) {
    Map<String, String> params = refreshParam(options.refresh());
    String path;
    String method;
    if (id == null) {
      path = "/" + index + "/_doc";
      method = "POST";
    } else {
      path = "/" + index + (options.createOnly() ? "/_create/" : "/_doc/")
          + pathSegment(id);
      method = "PUT";
    }
    return indexResult(object(require(perform(method, path, params, Json.canonical(document),
        RawHttp.JSON))));
  }

  @Override
  public IndexResult update(String index, String id, Map<String, Object> partial) {
    String path = "/" + index + "/_update/" + pathSegment(id);
    return indexResult(object(require(perform("POST", path, Map.of(),
        Json.canonical(Map.of("doc", partial)), RawHttp.JSON))));
  }

  @Override
  public Optional<Hit> get(String index, String id) {
    RawHttp.Response response =
        getRequest("/" + index + "/_doc/" + pathSegment(id), Map.of());
    if (response.status() == 404) {
      return Optional.empty();
    }
    Map<String, Object> doc = object(require(response));
    if (Boolean.FALSE.equals(doc.get("found"))) {
      return Optional.empty();
    }
    return Optional.of(new Hit(string(doc.get("_index")), string(doc.get("_id")), null,
        map(doc.get("_source")), List.of()));
  }

  @Override
  public boolean delete(String index, String id, IndexOptions options) {
    RawHttp.Response response = perform("DELETE", "/" + index + "/_doc/" + pathSegment(id),
        refreshParam(options.refresh()), null, RawHttp.JSON);
    if (response.status() == 404) {
      return false;
    }
    require(response);
    return true;
  }

  @Override
  public BulkResult bulk(List<BulkOp> operations) {
    StringBuilder ndjson = new StringBuilder();
    for (BulkOp op : operations) {
      Map<String, Object> meta = new TreeMap<>();
      meta.put("_index", op.index());
      if (op.id() != null) {
        meta.put("_id", op.id());
      }
      String verb = op.type().name().toLowerCase(java.util.Locale.ROOT);
      ndjson.append(Json.canonical(Map.of(verb, meta))).append('\n');
      if (op.type() == BulkOp.Type.UPDATE) {
        ndjson.append(Json.canonical(Map.of("doc", op.source()))).append('\n');
      } else if (op.type() != BulkOp.Type.DELETE) {
        ndjson.append(Json.canonical(op.source())).append('\n');
      }
    }
    Map<String, Object> response =
        object(require(perform("POST", "/_bulk", Map.of(), ndjson.toString(), RawHttp.NDJSON)));
    List<BulkResult.Item> items = new ArrayList<>();
    for (Map<String, Object> wrapper : list(response.get("items"))) {
      for (Map.Entry<String, Object> entry : wrapper.entrySet()) {
        Map<String, Object> item = map(entry.getValue());
        Map<String, Object> error = map(item.get("error"));
        items.add(new BulkResult.Item(
            entry.getKey(),
            string(item.get("_index")),
            string(item.get("_id")),
            (int) number(item.get("status")),
            error.isEmpty()
                ? null
                : string(error.get("type")) + ": " + string(error.get("reason"))));
      }
    }
    return new BulkResult(Boolean.TRUE.equals(response.get("errors")), items,
        number(response.get("took")));
  }

  @Override
  public SearchResult search(List<String> indices, Map<String, Object> body) {
    String path = body.containsKey("pit") || indices.isEmpty()
        ? "/_search"
        : "/" + String.join(",", indices) + "/_search";
    Map<String, Object> response =
        object(require(perform("POST", path, Map.of(), Json.canonical(body), RawHttp.JSON)));
    Map<String, Object> hits = map(response.get("hits"));
    Map<String, Object> total = map(hits.get("total"));
    List<Hit> parsed = new ArrayList<>();
    for (Map<String, Object> hit : list(hits.get("hits"))) {
      Object score = hit.get("_score");
      parsed.add(new Hit(
          string(hit.get("_index")),
          string(hit.get("_id")),
          score instanceof Number ? ((Number) score).doubleValue() : null,
          map(hit.get("_source")),
          hit.get("sort") instanceof List ? List.copyOf((List<?>) hit.get("sort")) : List.of()));
    }
    return new SearchResult(
        number(total.get("value")),
        !"gte".equals(total.get("relation")),
        parsed,
        string(response.get("pit_id")),
        map(response.get("aggregations")),
        number(response.get("took")));
  }

  @Override
  public Pit openPit(List<String> indices, Duration keepAlive) {
    String target = "/" + String.join(",", indices);
    Map<String, String> params = Map.of("keep_alive", timeValue(keepAlive));
    Map<String, Object> response;
    String idKey;
    if (info().product() == EngineProduct.OPENSEARCH) {
      response = object(require(perform("POST", target + "/_search/point_in_time", params, null,
          RawHttp.JSON)));
      idKey = "pit_id";
    } else {
      response = object(require(perform("POST", target + "/_pit", params, null, RawHttp.JSON)));
      idKey = "id";
    }
    return new Pit(string(response.get(idKey)), keepAlive);
  }

  @Override
  public void closePit(Pit pit) {
    if (info().product() == EngineProduct.OPENSEARCH) {
      require(perform("DELETE", "/_search/point_in_time", Map.of(),
          Json.canonical(Map.of("pit_id", List.of(pit.id()))), RawHttp.JSON));
    } else {
      require(perform("DELETE", "/_pit", Map.of(), Json.canonical(Map.of("id", pit.id())),
          RawHttp.JSON));
    }
  }

  @Override
  public void refresh(String index) {
    require(perform("POST", "/" + index + "/_refresh", Map.of(), null, RawHttp.JSON));
  }

  @Override
  public ClusterHealth health() {
    Map<String, Object> response = object(require(getRequest("/_cluster/health", Map.of())));
    return new ClusterHealth(
        string(response.get("status")),
        (int) number(response.get("number_of_nodes")),
        (int) number(response.get("number_of_data_nodes")));
  }

  @Override
  public void close() {
    try {
      http.close();
    } catch (IOException e) {
      throw new EngineException(Kind.IO, 0, "Failed to close the transport", e);
    }
  }

  /**
   * Formats a duration the way the engines expect time values: whole minutes, whole seconds or
   * milliseconds.
   */
  public static String timeValue(Duration duration) {
    long millis = duration.toMillis();
    if (millis % 60_000 == 0) {
      return millis / 60_000 + "m";
    }
    if (millis % 1_000 == 0) {
      return millis / 1_000 + "s";
    }
    return millis + "ms";
  }

  /**
   * Spells the flattened type the way the cluster expects. The product is only looked up when the
   * mappings use the type.
   */
  private Map<String, Object> toEngine(Map<String, Object> mappings) {
    if (!usesType(mappings, FLATTENED) || info().product() != EngineProduct.OPENSEARCH) {
      return mappings;
    }
    return renameType(mappings, FLATTENED, FLAT_OBJECT);
  }

  private static boolean usesType(Object node, String type) {
    if (node instanceof Map) {
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) node).entrySet()) {
        if ("_meta".equals(entry.getKey())) {
          continue;
        }
        if (("type".equals(entry.getKey()) && type.equals(entry.getValue()))
            || usesType(entry.getValue(), type)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Returns a copy of a mappings tree with every {@code "type": from} replaced by {@code to}. */
  private static Map<String, Object> renameType(Map<String, Object> node, String from,
      String to) {
    if (!usesType(node, from)) {
      return node;
    }
    Map<String, Object> copy = new TreeMap<>();
    for (Map.Entry<String, Object> entry : node.entrySet()) {
      Object value = entry.getValue();
      if ("type".equals(entry.getKey()) && from.equals(value)) {
        value = to;
      } else if (value instanceof Map && !"_meta".equals(entry.getKey())) {
        value = renameType(map(value), from, to);
      }
      copy.put(entry.getKey(), value);
    }
    return copy;
  }

  private Map<String, Object> single(String aliasOrIndex, String suffix) {
    Map<String, Object> response =
        object(require(getRequest("/" + aliasOrIndex + suffix, Map.of())));
    if (response.size() != 1) {
      throw new EngineException(Kind.CONFLICT, 0,
          aliasOrIndex + " resolves to " + response.size() + " indices: " + response.keySet());
    }
    return map(response.values().iterator().next());
  }

  private static Map<String, String> refreshParam(IndexOptions.Refresh refresh) {
    switch (refresh) {
      case IMMEDIATE:
        return Map.of("refresh", "true");
      case WAIT_FOR:
        return Map.of("refresh", "wait_for");
      default:
        return Map.of();
    }
  }

  private static IndexResult indexResult(Map<String, Object> response) {
    return new IndexResult(
        string(response.get("_index")),
        string(response.get("_id")),
        string(response.get("result")),
        number(response.get("_version")));
  }

  private RawHttp.Response getRequest(String path, Map<String, String> params) {
    return perform("GET", path, params, null, RawHttp.JSON);
  }

  private RawHttp.Response perform(
      String method, String path, Map<String, String> params, String body, String contentType) {
    try {
      return http.perform(method, path, params, body, contentType);
    } catch (IOException e) {
      String endpoint = http.describe();
      String target = endpoint == null || endpoint.isEmpty() ? "" : " on " + endpoint;
      throw new EngineException(Kind.IO, 0,
          method + " " + path + target + " failed: " + e.getMessage(), e);
    }
  }

  private static RawHttp.Response require(RawHttp.Response response) {
    if (response.ok()) {
      return response;
    }
    String type = "";
    String reason = response.body();
    try {
      Map<String, Object> error = map(Json.parseObject(response.body()).get("error"));
      if (!error.isEmpty()) {
        type = string(error.get("type"));
        reason = string(error.get("reason"));
      }
    } catch (IllegalArgumentException ignored) {
      // not a JSON error body; keep the raw text
    }
    throw new EngineException(classify(response.status(), type), response.status(),
        type.isEmpty() ? reason : type + ": " + reason);
  }

  private static Kind classify(int status, String type) {
    if (type.contains("already_exists")) {
      return Kind.ALREADY_EXISTS;
    }
    if (type.contains("not_found")) {
      return Kind.NOT_FOUND;
    }
    switch (status) {
      case 400:
        return Kind.BAD_REQUEST;
      case 401:
      case 403:
        return Kind.UNAUTHORIZED;
      case 404:
        return Kind.NOT_FOUND;
      case 409:
        return Kind.CONFLICT;
      case 408:
      case 504:
        return Kind.TIMEOUT;
      case 429:
      case 502:
      case 503:
        return Kind.TRANSIENT;
      default:
        return Kind.OTHER;
    }
  }

  private void sleep() {
    try {
      TimeUnit.NANOSECONDS.sleep(reindexPoll.toNanos());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new EngineException(Kind.OTHER, 0, "Interrupted while waiting for reindex", e);
    }
  }

  private static String pathSegment(String segment) {
    return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private static Map<String, Object> object(RawHttp.Response response) {
    return response.body() == null || response.body().isBlank()
        ? Map.of()
        : Json.parseObject(response.body());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : Collections.emptyMap();
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> list(Object value) {
    List<Map<String, Object>> out = new ArrayList<>();
    if (value instanceof List) {
      for (Object item : (List<Object>) value) {
        out.add(map(item));
      }
    }
    return out;
  }

  private static String string(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private static long number(Object value) {
    return value instanceof Number ? ((Number) value).longValue() : 0L;
  }
}
