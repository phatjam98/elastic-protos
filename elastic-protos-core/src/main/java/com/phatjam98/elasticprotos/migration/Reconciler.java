package com.phatjam98.elasticprotos.migration;

import com.phatjam98.elasticprotos.engine.BulkOp;
import com.phatjam98.elasticprotos.engine.BulkResult;
import com.phatjam98.elasticprotos.engine.EngineException;
import com.phatjam98.elasticprotos.engine.Hit;
import com.phatjam98.elasticprotos.engine.Pit;
import com.phatjam98.elasticprotos.engine.ReindexRequest;
import com.phatjam98.elasticprotos.engine.ReindexResult;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.engine.SearchResult;
import com.phatjam98.elasticprotos.util.Json;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Brings the new index of a zero-downtime reindex up to date with the frozen old index after the
 * alias swap, using a point in time over the new index taken just before the swap (the snapshot)
 * and the sequence number checkpoint of the old index taken before the first copy.
 *
 * <p>Reconcile, hot pass first: every document of the old index written after the checkpoint
 * ({@code _seq_no} above it) may have changed after the copy, whatever its version says (a version
 * restarts at 1 once a delete's tombstone is gone): it is created in the new index when the
 * snapshot lacks it, and otherwise, when its version or source differs from the snapshot's (with a
 * migration script: always), written on the snapshot's sequence number and primary term.
 * Then the cold pass: every other document is compared with the snapshot by version (or, for a
 * rollback, by source) and written the same way when it differs. A conditional write fails with
 * 409 when a write through the alias after the swap got there first, and that write wins.
 *
 * <p>Sweep: every document of the snapshot that the old index no longer has was deleted there after
 * the copy, and is deleted from the new index on the snapshot's sequence number.
 *
 * <p>The old index is scanned without sources; sources are fetched only for the documents that
 * are written.
 */
final class Reconciler {

  /** Documents per page and per bulk request. */
  static final int PAGE = 1000;

  private final SearchEngine engine;
  private final Duration keepAlive;
  private final Duration timeout;
  private final List<Object> sort;

  Reconciler(SearchEngine engine, Duration keepAlive, Duration timeout) {
    this.engine = engine;
    this.keepAlive = keepAlive;
    this.timeout = timeout;
    this.sort = List.of(Map.of(engine.info().supportsShardDoc() ? "_shard_doc" : "_id", "asc"));
  }

  /** The point in time over the new index is gone, so its snapshot cannot be read. */
  static final class SnapshotLost extends RuntimeException {
    private static final long serialVersionUID = 1L;

    SnapshotLost(Throwable cause) {
      super("the point in time over the new index is gone: " + cause.getMessage(), cause);
    }
  }

  /**
   * A migration script applied to reconciled documents through a staging index.
   *
   * @param index the staging index
   * @param script the Painless source
   * @param params its parameters
   * @param settings the settings to create the staging index with
   */
  record Staging(String index, String script, Map<String, Object> params,
      Map<String, Object> settings) {}

  /** Running totals. */
  static final class Counts {
    long copied;
    long superseded;
    long ghostsDeleted;
    long staleSkipped;
    long missing;
    final List<String> warnings = new ArrayList<>();

    ReconcileStats stats() {
      return new ReconcileStats(copied, superseded, ghostsDeleted, staleSkipped);
    }
  }

  // ------------------------------------------------------------------ reconcile

  /**
   * Copies the documents of {@code from} that changed after the checkpoint, or that the snapshot of
   * {@code to} lacks or holds in another version.
   *
   * @param checkpoint the sequence number checkpoint of {@code from} taken before the first copy;
   *     negative when unknown, which makes every document hot
   * @param staging the script to apply, or null
   * @param compareSources whether cold documents are compared by source instead of version, for a
   *     rollback, whose versions are not comparable across the indices
   * @throws SnapshotLost when the snapshot's point in time is gone
   */
  void reconcile(String from, String to, String snapshotPit, long checkpoint, Staging staging,
      boolean compareSources, Counts counts) {
    // without a script, a hot document is compared by version and source; with one, the copy's
    // source differs by design, so every hot document is written
    boolean bySource = staging == null;
    Map<String, Object> hot = new LinkedHashMap<>();
    hot.put("_source", bySource);
    hot.put("version", true);
    hot.put("seq_no_primary_term", true);
    if (checkpoint >= 0) {
      hot.put("query", Map.of("range", Map.of("_seq_no", Map.of("gt", checkpoint))));
    }
    scan(from, hot, page -> {
      Map<String, Hit> snapshot = snapshot(snapshotPit, ids(page), bySource);
      List<String> changed = new ArrayList<>();
      for (Hit hit : page) {
        Hit seen = snapshot.get(hit.id());
        if (!bySource || seen == null || differs(hit, seen, false) || differs(hit, seen, true)) {
          changed.add(hit.id());
        }
      }
      write(from, to, changed, snapshot, staging, counts);
    });
    if (checkpoint < 0) {
      return;
    }
    Map<String, Object> cold = new LinkedHashMap<>();
    cold.put("_source", compareSources);
    cold.put("version", true);
    cold.put("seq_no_primary_term", true);
    scan(from, cold, page -> {
      List<Hit> unchanged = new ArrayList<>();
      for (Hit hit : page) {
        if (hit.seqNo() == null || hit.seqNo() <= checkpoint) {
          unchanged.add(hit);
        }
      }
      if (unchanged.isEmpty()) {
        return;
      }
      Map<String, Hit> snapshot = snapshot(snapshotPit, ids(unchanged), compareSources);
      List<String> changed = new ArrayList<>();
      for (Hit hit : unchanged) {
        Hit seen = snapshot.get(hit.id());
        if (seen == null || differs(hit, seen, compareSources)) {
          changed.add(hit.id());
        }
      }
      write(from, to, changed, snapshot, staging, counts);
    });
  }

  /** Writes documents of {@code from} into {@code to}, conditional on the snapshot. */
  private void write(String from, String to, List<String> ids, Map<String, Hit> snapshot,
      Staging staging, Counts counts) {
    if (ids.isEmpty()) {
      return;
    }
    Map<String, Map<String, Object>> sources = staging == null
        ? sources(from, ids) : stage(from, staging, ids);
    List<BulkOp> ops = new ArrayList<>();
    List<BulkOp> deletes = new ArrayList<>();
    for (String id : ids) {
      Hit seen = snapshot.get(id);
      Map<String, Object> source = sources.get(id);
      if (source == null) {
        // the script dropped the document: drop the copy the snapshot holds, if any
        if (seen != null) {
          deletes.add(BulkOp.deleteIfMatch(to, id, seqNo(seen), primaryTerm(seen)));
        }
      } else if (seen == null) {
        ops.add(BulkOp.create(to, id, source));
      } else {
        ops.add(BulkOp.indexIfMatch(to, id, source, seqNo(seen), primaryTerm(seen)));
      }
    }
    apply(ops, counts, false);
    apply(deletes, counts, true);
  }

  /**
   * Deletes from {@code to} the documents of the snapshot that {@code from} no longer has.
   *
   * @throws SnapshotLost when the snapshot's point in time is gone
   */
  void sweep(String from, String to, String snapshotPit, Counts counts) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("_source", false);
    body.put("seq_no_primary_term", true);
    scanPit(snapshotPit, body, true, page -> {
      Set<String> present = present(from, ids(page));
      List<BulkOp> deletes = new ArrayList<>();
      for (Hit hit : page) {
        if (!present.contains(hit.id())) {
          deletes.add(BulkOp.deleteIfMatch(to, hit.id(), seqNo(hit), primaryTerm(hit)));
        }
      }
      apply(deletes, counts, true);
    });
  }

  /**
   * Compares without the snapshot. A write through the alias after the swap cannot be told apart
   * from a stale copy, so documents that differ are only counted ({@code staleSkipped}), and so
   * are those that exist only in {@code to}. Documents {@code to} lacks are counted ({@code
   * missing}) or, with {@code createMissing}, created, which brings back every document deleted
   * through the alias since the swap.
   */
  void degraded(String from, String to, Staging staging, boolean compareSources,
      boolean createMissing, Counts counts) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("version", true);
    body.put("_source", compareSources);
    scan(from, body, page -> {
      Map<String, Hit> live = lookup(to, ids(page), compareSources);
      List<String> missing = new ArrayList<>();
      for (Hit hit : page) {
        Hit current = live.get(hit.id());
        if (current == null) {
          missing.add(hit.id());
        } else if (differs(hit, current, compareSources)) {
          counts.staleSkipped++;
        }
      }
      if (missing.isEmpty()) {
        return;
      }
      if (!createMissing) {
        counts.missing += missing.size();
        return;
      }
      Map<String, Map<String, Object>> sources = staging == null
          ? sources(from, missing) : stage(from, staging, missing);
      List<BulkOp> ops = new ArrayList<>();
      sources.forEach((id, source) -> ops.add(BulkOp.create(to, id, source)));
      apply(ops, counts, false);
    });
    long[] onlyInNew = {0};
    Map<String, Object> ghostBody = new LinkedHashMap<>();
    ghostBody.put("_source", false);
    scan(to, ghostBody, page -> {
      Set<String> present = present(from, ids(page));
      for (Hit hit : page) {
        if (!present.contains(hit.id())) {
          onlyInNew[0]++;
        }
      }
    });
    String lost = "the snapshot of " + to + " taken at the swap was lost";
    if (createMissing) {
      counts.warnings.add(lost + ", so missing documents were created (including any deleted"
          + " through the alias since the swap): " + counts.copied + " created, "
          + counts.staleSkipped + " documents differ between " + from + " and " + to
          + " and were left as they are in " + to + ", and " + onlyInNew[0]
          + " documents exist only in " + to + " and were kept");
    } else {
      counts.warnings.add(lost + "; nothing was written. " + counts.missing + " documents of "
          + from + " are missing from " + to + ", " + counts.staleSkipped + " differ, and "
          + onlyInNew[0] + " exist only in " + to + ". Missing documents may have been written"
          + " before the swap or deleted through the alias after it; apply again with"
          + " DegradedReconcile.CREATE_MISSING to create them, or remove the pending marker");
    }
  }

  // ------------------------------------------------------------------ staging

  /**
   * Runs the script over the first page of documents of {@code from} into the staging index and
   * checks that it kept their ids, before the alias moves.
   *
   * @throws MigrationException when the script changed a document id
   */
  void probe(String from, Staging staging) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("_source", false);
    body.put("size", PAGE);
    body.put("track_total_hits", false);
    List<String> ids = ids(engine.search(List.of(from), body).hits());
    if (ids.isEmpty()) {
      return;
    }
    try {
      stage(from, staging, ids);
    } catch (IdChanged e) {
      throw new MigrationException("the migration script changes _id; use ReindexMode.BLOCKING"
          + " or fix the script (" + e.getMessage() + ")");
    }
  }

  /** The script changed document ids. */
  private static final class IdChanged extends MigrationException {
    private static final long serialVersionUID = 1L;

    IdChanged(String message) {
      super(message);
    }
  }

  /**
   * Runs the script over the given documents of {@code from} into the staging index and returns
   * the staged sources by id. Documents the script dropped are absent.
   *
   * @throws MigrationException when the script changed a document id
   */
  private Map<String, Map<String, Object>> stage(String from, Staging staging, List<String> ids) {
    ReindexRequest request = ReindexRequest.of(from, staging.index())
        .withQuery(idsQuery(ids))
        .withScript(staging.script(), staging.params())
        .withRefresh(true)
        .withTimeout(timeout);
    ReindexResult result = engine.reindex(request);
    if (!result.complete()) {
      throw new MigrationException("staging " + ids.size() + " documents of " + from + " through"
          + " the migration script into " + staging.index() + " did not complete: "
          + IndexMigrator.describe(result));
    }
    Map<String, Map<String, Object>> staged = fetch(staging.index(), ids);
    if (staged.size() != result.created() + result.updated()) {
      throw new IdChanged("the migration script wrote " + (result.created()
          + result.updated()) + " documents into " + staging.index() + " but only "
          + staged.size() + " kept their ids: a script must not change _id during a"
          + " zero-downtime reindex; use ReindexMode.BLOCKING");
    }
    return staged;
  }

  // ------------------------------------------------------------------ helpers

  /** Fetches the sources of documents of the frozen old index. */
  private Map<String, Map<String, Object>> sources(String index, List<String> ids) {
    return fetch(index, ids);
  }

  private Map<String, Map<String, Object>> fetch(String index, List<String> ids) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("query", idsQuery(ids));
    body.put("size", ids.size());
    body.put("track_total_hits", false);
    Map<String, Map<String, Object>> found = new LinkedHashMap<>();
    for (Hit hit : engine.search(List.of(index), body).hits()) {
      found.put(hit.id(), hit.source());
    }
    return found;
  }

  /** Writes the operations and counts their outcomes; other failures stop the reconcile. */
  private void apply(List<BulkOp> ops, Counts counts, boolean deletes) {
    if (ops.isEmpty()) {
      return;
    }
    BulkResult result = engine.bulk(ops);
    List<BulkResult.Item> failed = new ArrayList<>();
    for (BulkResult.Item item : result.items()) {
      if (item.ok() && item.status() != 404) {
        if (deletes) {
          counts.ghostsDeleted++;
        } else {
          counts.copied++;
        }
      } else if (item.status() == 409 || item.status() == 404) {
        counts.superseded++;
      } else {
        failed.add(item);
      }
    }
    if (!failed.isEmpty()) {
      BulkResult.Item first = failed.get(0);
      throw new MigrationException(failed.size() + " of " + ops.size() + " reconcile writes"
          + " failed, first " + first.op() + " " + first.index() + "/" + first.id() + ": "
          + first.status() + " " + first.error());
    }
  }

  private static boolean differs(Hit old, Hit snapshot, boolean compareSources) {
    if (compareSources) {
      return !Json.canonical(old.source()).equals(Json.canonical(snapshot.source()));
    }
    return old.version() == null || !old.version().equals(snapshot.version());
  }

  /** Looks the ids up in the snapshot. */
  private Map<String, Hit> snapshot(String pitId, List<String> ids, boolean withSource) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("pit", pit(pitId));
    body.put("query", idsQuery(ids));
    body.put("version", true);
    body.put("seq_no_primary_term", true);
    body.put("_source", withSource);
    body.put("size", ids.size());
    body.put("track_total_hits", false);
    Map<String, Hit> found = new LinkedHashMap<>();
    for (Hit hit : searchPit(body).hits()) {
      found.put(hit.id(), hit);
    }
    return found;
  }

  /** Looks the ids up in a live index. */
  private Map<String, Hit> lookup(String index, List<String> ids, boolean withSource) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("query", idsQuery(ids));
    body.put("version", true);
    body.put("_source", withSource);
    body.put("size", ids.size());
    body.put("track_total_hits", false);
    Map<String, Hit> found = new LinkedHashMap<>();
    for (Hit hit : engine.search(List.of(index), body).hits()) {
      found.put(hit.id(), hit);
    }
    return found;
  }

  private Set<String> present(String index, List<String> ids) {
    return new HashSet<>(lookup(index, ids, false).keySet());
  }

  /** Pages through an index under a point in time of its own. */
  private void scan(String index, Map<String, Object> body, Consumer<List<Hit>> pages) {
    Pit pit = engine.openPit(List.of(index), keepAlive);
    try {
      scanPit(pit.id(), body, false, pages);
    } finally {
      try {
        engine.closePit(pit);
      } catch (RuntimeException e) {
        // it expires on its own
      }
    }
  }

  /** Pages through a point in time with {@code search_after}. */
  private void scanPit(String pitId, Map<String, Object> body, boolean snapshot,
      Consumer<List<Hit>> pages) {
    List<Object> after = null;
    while (true) {
      Map<String, Object> request = new LinkedHashMap<>(body);
      request.put("pit", pit(pitId));
      request.put("size", PAGE);
      request.put("sort", sort);
      request.put("track_total_hits", false);
      if (after != null) {
        request.put("search_after", after);
      }
      SearchResult result = snapshot ? searchPit(request) : engine.search(List.of(), request);
      if (result.hits().isEmpty()) {
        return;
      }
      pages.accept(result.hits());
      if (result.hits().size() < PAGE) {
        return;
      }
      after = result.lastSort();
    }
  }

  /** Searches the snapshot, turning a missing point in time into {@link SnapshotLost}. */
  private SearchResult searchPit(Map<String, Object> body) {
    try {
      return engine.search(List.of(), body);
    } catch (EngineException e) {
      if (e.getKind() == EngineException.Kind.NOT_FOUND) {
        throw new SnapshotLost(e);
      }
      throw e;
    }
  }

  private Map<String, Object> pit(String id) {
    Map<String, Object> pit = new LinkedHashMap<>();
    pit.put("id", id);
    pit.put("keep_alive", keepAlive.toSeconds() + "s");
    return pit;
  }

  private static Map<String, Object> idsQuery(List<String> ids) {
    return Map.of("ids", Map.of("values", ids));
  }

  private static List<String> ids(List<Hit> page) {
    List<String> ids = new ArrayList<>();
    for (Hit hit : page) {
      ids.add(hit.id());
    }
    return ids;
  }

  private static long seqNo(Hit hit) {
    if (hit.seqNo() == null) {
      throw new MigrationException("the engine returned no sequence number for " + hit.index()
          + "/" + hit.id());
    }
    return hit.seqNo();
  }

  private static long primaryTerm(Hit hit) {
    if (hit.primaryTerm() == null) {
      throw new MigrationException("the engine returned no primary term for " + hit.index()
          + "/" + hit.id());
    }
    return hit.primaryTerm();
  }
}
