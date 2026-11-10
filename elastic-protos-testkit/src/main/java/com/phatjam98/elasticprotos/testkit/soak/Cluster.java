package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.elasticsearch.ElasticsearchEngine;
import com.phatjam98.elasticprotos.engine.BulkOp;
import com.phatjam98.elasticprotos.engine.BulkResult;
import com.phatjam98.elasticprotos.engine.Hit;
import com.phatjam98.elasticprotos.engine.Pit;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.engine.SearchResult;
import com.phatjam98.elasticprotos.mapping.IndexNamingStrategy;
import com.phatjam98.elasticprotos.mapping.ProtoMapper;
import com.phatjam98.elasticprotos.opensearch.OpenSearchEngine;
import com.phatjam98.elasticprotos.testkit.soak.OpLog.Entry;
import com.phatjam98.elasticprotos.testkit.soak.OpLog.OpType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Cluster helpers shared by the soak scenarios. */
public final class Cluster {

  private Cluster() {}

  /**
   * Connects to a cluster.
   *
   * @param product {@code elasticsearch} or {@code opensearch}
   * @param url the cluster URL
   * @return the engine
   */
  public static SearchEngine connect(String product, String url) {
    switch (product.toLowerCase(Locale.ROOT)) {
      case "elasticsearch":
        return ElasticsearchEngine.builder().hosts(url).build();
      case "opensearch":
        return OpenSearchEngine.builder().hosts(url).build();
      default:
        throw new IllegalArgumentException(
            "unknown engine " + product + "; expected elasticsearch or opensearch");
    }
  }

  /** Returns the concrete indices whose names start with {@code prefix-}, with their aliases. */
  public static Map<String, Set<String>> indices(SearchEngine engine, String prefix) {
    return new TreeMap<>(engine.getAliases(prefix + "-*"));
  }

  /** Deletes every index whose name starts with {@code prefix-}. */
  public static void clean(SearchEngine engine, String prefix) {
    for (String index : indices(engine, prefix).keySet()) {
      engine.deleteIndex(index);
    }
  }

  /** Returns the indices an alias points at. */
  public static Set<String> holders(SearchEngine engine, String alias) {
    Set<String> holders = new TreeSet<>();
    engine.getAliases(alias).forEach((index, names) -> {
      if (names.contains(alias)) {
        holders.add(index);
      }
    });
    return holders;
  }

  /** Returns the single index an alias points at. */
  public static String holder(SearchEngine engine, String alias) {
    Set<String> holders = holders(engine, alias);
    if (holders.size() != 1) {
      throw new IllegalStateException("alias " + alias + " points at " + holders);
    }
    return holders.iterator().next();
  }

  /** Returns {@code _meta.elastic_protos.pending} of an index, or an empty map. */
  public static Map<String, Object> pending(SearchEngine engine, String index) {
    return map(map(map(engine.getMappings(index).get("_meta")).get(ProtoMapper.META_KEY))
        .get("pending"));
  }

  /** Returns the lock index of a prefix. */
  public static String lockIndex(String prefix) {
    return IndexNamingStrategy.prefixed(prefix).lockIndex();
  }

  /** Returns the lock document of an alias, or empty. */
  public static Optional<Hit> lock(SearchEngine engine, String prefix, String alias) {
    String index = lockIndex(prefix);
    if (!engine.indexExists(index)) {
      return Optional.empty();
    }
    return engine.get(index, alias);
  }

  /** Counts the documents in the lock index of a prefix, after a refresh; 0 when absent. */
  public static long lockCount(SearchEngine engine, String prefix) {
    String index = lockIndex(prefix);
    if (!engine.indexExists(index)) {
      return 0;
    }
    engine.refresh(index);
    return engine.count(index);
  }

  /**
   * Bulk-indexes {@code count} seeded documents through the alias and records them in the log.
   *
   * @param engine the engine
   * @param alias the alias
   * @param count how many
   * @param log the log, or null
   * @param seed the random seed
   */
  public static void seed(SearchEngine engine, String alias, int count, OpLog log, long seed) {
    Random random = new Random(seed);
    int batch = 2000;
    for (int from = 0; from < count; from += batch) {
      List<BulkOp> ops = new ArrayList<>();
      Map<String, Map<String, Object>> docs = new HashMap<>();
      for (int n = from; n < Math.min(count, from + batch); n++) {
        String id = Writers.seedId(n);
        Map<String, Object> doc = SoakSchema.document(id, n, random);
        docs.put(id, doc);
        ops.add(BulkOp.index(alias, id, doc));
      }
      long started = System.currentTimeMillis();
      BulkResult result = engine.bulk(ops);
      long acked = System.currentTimeMillis();
      if (result.errors()) {
        throw new IllegalStateException("seeding failed: " + result.failures().get(0));
      }
      if (log != null) {
        for (BulkResult.Item item : result.items()) {
          log.ack(item.id(), new Entry(OpType.SEED, docs.get(item.id()), started, acked,
              item.index(), false));
        }
      }
    }
    engine.refresh(alias);
  }

  /**
   * Reads every document behind an alias under a point in time, sorted by {@code _shard_doc}
   * where the engine supports it and by {@code _id} otherwise (so nothing depends on a document
   * field), and cross-checks the number read with an exact count. Call it once writes have
   * stopped.
   *
   * @param engine the engine
   * @param alias the alias
   * @return the documents by {@code _id}
   * @throws IllegalStateException when a document comes back twice or the count differs
   */
  public static Map<String, Map<String, Object>> scan(SearchEngine engine, String alias) {
    engine.refresh(alias);
    long counted = engine.count(alias);
    Map<String, Map<String, Object>> docs = new HashMap<>();
    Object sortField = engine.info().supportsShardDoc() ? "_shard_doc" : "_id";
    Pit pit = engine.openPit(List.of(alias), Duration.ofMinutes(10));
    try {
      List<Object> after = null;
      while (true) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("pit", Map.of("id", pit.id(), "keep_alive", "10m"));
        body.put("size", 5000);
        body.put("sort", List.of(Map.of(sortField, "asc")));
        body.put("track_total_hits", false);
        if (after != null) {
          body.put("search_after", after);
        }
        SearchResult result = engine.search(List.of(), body);
        for (Hit hit : result.hits()) {
          if (docs.put(hit.id(), hit.source()) != null) {
            throw new IllegalStateException("document " + hit.id() + " returned twice");
          }
        }
        if (result.hits().size() < 5000) {
          break;
        }
        after = result.lastSort();
      }
    } finally {
      try {
        engine.closePit(pit);
      } catch (RuntimeException e) {
        // it expires on its own
      }
    }
    if (docs.size() != counted) {
      throw new IllegalStateException("scan of " + alias + " read " + docs.size()
          + " documents but the count is " + counted);
    }
    return docs;
  }

  /** Returns whether an index has {@code index.blocks.write} set. */
  public static boolean writeBlocked(SearchEngine engine, String index) {
    Object write = map(map(map(engine.getSettings(index).get("index")).get("blocks")))
        .get("write");
    return "true".equals(String.valueOf(write));
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> map(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : Map.of();
  }
}
