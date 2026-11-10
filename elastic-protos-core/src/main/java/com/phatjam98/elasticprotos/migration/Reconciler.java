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
import java.util.OptionalLong;
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
 * <p>With a sound checkpoint ({@link ReconcileStrategy#SEQ_NO_RANGE}) the cold pass is left out,
 * and {@link #ghosts} counts the documents the sweep would delete from the document counts alone,
 * so that the sweep runs only when there are any.
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
  private final Runnable guard;
  private final Progress progress;
  private final Consumer<Progress> save;
  private final int savePages;
  private final Duration saveInterval;

  /**
   * Creates a reconciler that keeps no progress, for the probe.
   *
   * @param guard run before every page is written; throws to stop, for a lost alias lock
   */
  Reconciler(SearchEngine engine, Duration keepAlive, Duration timeout, Runnable guard) {
    this(engine, keepAlive, timeout, guard, new Progress(), progress -> {});
  }

  /**
   * Creates a reconciler.
   *
   * @param guard run before every page is written; throws to stop, for a lost alias lock
   * @param progress where an earlier run stopped; the passes it completed are skipped and the
   *     pass it was in resumes after the last page whose writes it recorded
   * @param save records the progress, after every page that wrote and at the end of every pass
   */
  Reconciler(SearchEngine engine, Duration keepAlive, Duration timeout, Runnable guard,
      Progress progress, Consumer<Progress> save) {
    this(engine, keepAlive, timeout, guard, progress, save, SAVE_PAGES, SAVE_INTERVAL);
  }

  /**
   * Creates a reconciler that records its progress at another cadence, for tests.
   *
   * @param savePages record the cursor once this many pages wrote since the last record
   * @param saveInterval record the cursor after a page once this long passed since the last record
   */
  Reconciler(SearchEngine engine, Duration keepAlive, Duration timeout, Runnable guard,
      Progress progress, Consumer<Progress> save, int savePages, Duration saveInterval) {
    this.guard = guard;
    this.engine = engine;
    this.keepAlive = keepAlive;
    this.timeout = timeout;
    this.sort = List.of(Map.of(engine.info().supportsShardDoc() ? "_shard_doc" : "_id", "asc"));
    this.progress = progress;
    this.save = save;
    this.savePages = savePages;
    this.saveInterval = saveInterval;
  }

  /**
   * How many pages that wrote may go unrecorded: each record of the cursor is a mapping update, a
   * cluster-state publication, so recording after every page loads the master on large reconciles,
   * while every unrecorded page is replayed after a crash. Four pages (4,000 documents) keep the
   * mapping updates to one per 4,000 written documents at most.
   */
  static final int SAVE_PAGES = 4;

  /** How long the cursor may go unrecorded while pages are read, written or not. */
  static final Duration SAVE_INTERVAL = Duration.ofSeconds(5);

  /**
   * How far the reconcile got, kept in the marker ({@code pending.progress}) so that a resumed run
   * never repeats a write it already made and recorded. A replayed {@code create} would bring back
   * a document the earlier run created and the application then deleted through the alias, since
   * the snapshot still lacks it; conditional writes and deletes are safe to repeat (409), but
   * creates are not.
   *
   * <p>A pass reads the frozen old index ordered by {@code _seq_no} (then by shard), so its order
   * does not depend on the point in time or on merges. Each pass has its own cursor: the last
   * sequence number processed and the ids at it (sequence numbers are per shard, so several
   * documents can share one). A resumed pass reads from that sequence number on, without those
   * ids; a cursor of one pass is never touched by another (a resumed degraded pass keeps its
   * cursor while the run first tries the snapshot). Completed passes are listed in {@code done}.
   * {@code hot_missing} counts the hot documents the snapshot lacks, up to the hot cursor.
   */
  static final class Progress {
    final Set<String> done = new java.util.TreeSet<>();
    final Map<String, Cursor> cursors = new java.util.TreeMap<>();
    long hotMissing;

    /** Where a pass got to: the last sequence number, and the ids at it already processed. */
    static final class Cursor {
      long seqNo;
      final Set<String> ids = new java.util.TreeSet<>();
    }

    /** Reads the progress a marker records, or an empty one (markers of 1.0.0-rc.3). */
    static Progress from(Object value) {
      Progress progress = new Progress();
      if (!(value instanceof Map<?, ?> map)) {
        return progress;
      }
      if (map.get("done") instanceof List<?> done) {
        done.forEach(pass -> progress.done.add(String.valueOf(pass)));
      }
      if (map.get("cursors") instanceof Map<?, ?> cursors) {
        cursors.forEach((pass, raw) -> {
          if (raw instanceof Map<?, ?> fields && fields.get("seq_no") instanceof Number seqNo) {
            Cursor cursor = new Cursor();
            cursor.seqNo = seqNo.longValue();
            if (fields.get("ids") instanceof List<?> ids) {
              ids.forEach(id -> cursor.ids.add(String.valueOf(id)));
            }
            progress.cursors.put(String.valueOf(pass), cursor);
          }
        });
      }
      if (map.get("hot_missing") instanceof Number missing) {
        progress.hotMissing = missing.longValue();
      }
      return progress;
    }

    /** Returns the marker form; null when nothing happened yet. */
    Map<String, Object> toMap() {
      if (done.isEmpty() && cursors.isEmpty() && hotMissing == 0) {
        return null;
      }
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("done", new ArrayList<>(done));
      Map<String, Object> marks = new java.util.TreeMap<>();
      cursors.forEach((pass, cursor) -> {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("seq_no", cursor.seqNo);
        fields.put("ids", new ArrayList<>(cursor.ids));
        marks.put(pass, fields);
      });
      map.put("cursors", marks);
      map.put("hot_missing", hotMissing);
      return map;
    }

    boolean done(String name) {
      return done.contains(name);
    }

    void finish(String name) {
      done.add(name);
      cursors.remove(name);
    }
  }

  /** The shard failure of a point in time whose reader is gone on that shard. */
  static final String CONTEXT_MISSING = "search_context_missing_exception";

  static final String HOT = "hot";
  static final String COLD = "cold";
  static final String SWEEP = "sweep";
  static final String DEGRADED = "degraded";

  /** The point in time over the new index is gone, so its snapshot cannot be read. */
  static final class SnapshotLost extends RuntimeException {
    private static final long serialVersionUID = 1L;

    SnapshotLost(Throwable cause) {
      super("the point in time over the new index is gone: " + cause.getMessage(), cause);
    }

    SnapshotLost(String message) {
      super(message);
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
    ReconcileStrategy strategy = ReconcileStrategy.FULL_SCAN;
    boolean swept;
    final List<String> warnings = new ArrayList<>();

    ReconcileStats stats() {
      return new ReconcileStats(copied, superseded, ghostsDeleted, staleSkipped, strategy, swept);
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
    reconcileHot(from, to, snapshotPit, checkpoint, staging, counts);
    if (checkpoint >= 0) {
      reconcileCold(from, to, snapshotPit, checkpoint, staging, compareSources, counts);
    }
  }

  /**
   * The hot pass: copies the documents of {@code from} written after the checkpoint (every
   * document when it is negative) that the snapshot lacks or holds differently.
   *
   * @return how many of those documents the snapshot lacks
   * @throws SnapshotLost when the snapshot's point in time is gone
   */
  long reconcileHot(String from, String to, String snapshotPit, long checkpoint, Staging staging,
      Counts counts) {
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
    scanBySeqNo(HOT, from, hot, page -> {
      Map<String, Hit> snapshot = snapshot(snapshotPit, ids(page), bySource);
      List<String> changed = new ArrayList<>();
      for (Hit hit : page) {
        Hit seen = snapshot.get(hit.id());
        if (seen == null) {
          progress.hotMissing++;
        }
        if (!bySource || seen == null || differs(hit, seen, false) || differs(hit, seen, true)) {
          changed.add(hit.id());
        }
      }
      return write(from, to, changed, snapshot, staging, counts);
    });
    return progress.hotMissing;
  }

  /**
   * The cold pass: compares every document of {@code from} at or below the checkpoint with the
   * snapshot, by version or, with {@code compareSources}, by source, and copies those that differ.
   *
   * @throws SnapshotLost when the snapshot's point in time is gone
   */
  void reconcileCold(String from, String to, String snapshotPit, long checkpoint, Staging staging,
      boolean compareSources, Counts counts) {
    Map<String, Object> cold = new LinkedHashMap<>();
    cold.put("_source", compareSources);
    cold.put("version", true);
    cold.put("seq_no_primary_term", true);
    cold.put("query", Map.of("range", Map.of("_seq_no", Map.of("lte", checkpoint))));
    scanBySeqNo(COLD, from, cold, page -> {
      Map<String, Hit> snapshot = snapshot(snapshotPit, ids(page), compareSources);
      List<String> changed = new ArrayList<>();
      for (Hit hit : page) {
        Hit seen = snapshot.get(hit.id());
        if (seen == null || differs(hit, seen, compareSources)) {
          changed.add(hit.id());
        }
      }
      return write(from, to, changed, snapshot, staging, counts);
    });
  }

  /**
   * Writes documents of {@code from} into {@code to}, conditional on the snapshot.
   *
   * @return whether anything was written
   */
  private boolean write(String from, String to, List<String> ids, Map<String, Hit> snapshot,
      Staging staging, Counts counts) {
    if (ids.isEmpty()) {
      return false;
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
    return !ops.isEmpty() || !deletes.isEmpty();
  }

  /**
   * Counts the documents of the snapshot that the frozen old index no longer has, without reading
   * them: the snapshot's count, minus the old index's count, plus the hot documents the snapshot
   * lacks. This holds when every document of {@code from} at or below the checkpoint is in the
   * snapshot (the new index was created by the run that copied into it, with no script, after the
   * global checkpoint was taken), because the snapshot then holds every document of {@code from}
   * except those hot ones, and the ghosts besides.
   *
   * @param hotMissing what {@link #reconcileHot} returned
   * @return the number of ghosts, or empty when a count is not exact
   * @throws SnapshotLost when the snapshot's point in time is gone
   */
  OptionalLong ghosts(String from, String snapshotPit, long hotMissing) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("pit", pit(snapshotPit));
    body.put("size", 0);
    body.put("track_total_hits", true);
    SearchResult snapshot = searchPit(body);
    SearchResult old = search(List.of(from), Map.of("size", 0, "track_total_hits", true));
    if (!snapshot.totalExact() || !old.totalExact()) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(snapshot.total() - old.total() + hotMissing);
  }

  /**
   * Deletes from {@code to} the documents of the snapshot that {@code from} no longer has.
   *
   * @throws SnapshotLost when the snapshot's point in time is gone
   */
  void sweep(String from, String to, String snapshotPit, Counts counts) {
    if (progress.done(SWEEP)) {
      counts.swept = true;
      return;
    }
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
    counts.swept = true;
    progress.finish(SWEEP);
    save.accept(progress);
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
    counts.strategy = ReconcileStrategy.DEGRADED;
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("version", true);
    body.put("_source", compareSources);
    body.put("seq_no_primary_term", true);
    // only CREATE_MISSING writes, and only its progress is kept: REPORT must see everything again
    scanBySeqNo(createMissing ? DEGRADED : null, from, body, page -> {
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
        return false;
      }
      if (!createMissing) {
        counts.missing += missing.size();
        return false;
      }
      Map<String, Map<String, Object>> sources = staging == null
          ? sources(from, missing) : stage(from, staging, missing);
      List<BulkOp> ops = new ArrayList<>();
      sources.forEach((id, source) -> ops.add(BulkOp.create(to, id, source)));
      apply(ops, counts, false);
      return !ops.isEmpty();
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
    List<String> ids = ids(search(List.of(from), body).hits());
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
    for (Hit hit : search(List.of(index), body).hits()) {
      found.put(hit.id(), hit.source());
    }
    return found;
  }

  /** Writes the operations and counts their outcomes; other failures stop the reconcile. */
  private void apply(List<BulkOp> ops, Counts counts, boolean deletes) {
    if (ops.isEmpty()) {
      return;
    }
    guard.run();
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
    for (Hit hit : search(List.of(index), body).hits()) {
      found.put(hit.id(), hit);
    }
    return found;
  }

  private Set<String> present(String index, List<String> ids) {
    return new HashSet<>(lookup(index, ids, false).keySet());
  }

  /**
   * Runs a pass over the frozen old index in {@code _seq_no} order, resuming after its recorded
   * cursor and skipping it when it is done; records the cursor after a page that wrote once
   * {@link #savePages} pages wrote since the last record or {@link #saveInterval} passed, and the
   * pass as done at its end. A null {@code pass} keeps no progress.
   *
   * @param pages handles a page and returns whether it wrote anything
   */
  private void scanBySeqNo(String pass, String index, Map<String, Object> body,
      java.util.function.Function<List<Hit>, Boolean> pages) {
    if (pass != null && progress.done(pass)) {
      return;
    }
    Map<String, Object> request = new LinkedHashMap<>(body);
    request.put("seq_no_primary_term", true);
    Progress.Cursor resumeAt = pass == null ? null : progress.cursors.get(pass);
    if (resumeAt != null) {
      List<Object> filter = new ArrayList<>();
      if (body.get("query") != null) {
        filter.add(body.get("query"));
      }
      // gte, not gt: other shards' documents can share the cursor's sequence number
      filter.add(Map.of("range", Map.of("_seq_no", Map.of("gte", resumeAt.seqNo))));
      Map<String, Object> bool = new LinkedHashMap<>();
      bool.put("filter", filter);
      if (!resumeAt.ids.isEmpty()) {
        bool.put("must_not", List.of(idsQuery(new ArrayList<>(resumeAt.ids))));
      }
      request.put("query", Map.of("bool", bool));
    } else if (HOT.equals(pass)) {
      // the hot pass starts over: its count of missing documents starts over with it
      progress.hotMissing = 0;
    }
    List<Object> order = new ArrayList<>();
    order.add(Map.of("_seq_no", "asc"));
    order.addAll(sort);
    int[] unsaved = {0};
    long[] savedAt = {System.nanoTime()};
    Pit pit = engine.openPit(List.of(index), keepAlive);
    try {
      scanPit(pit.id(), request, false, order, page -> {
        final boolean wrote = pages.apply(page);
        if (pass == null) {
          return;
        }
        Progress.Cursor cursor = progress.cursors.computeIfAbsent(pass, key -> {
          Progress.Cursor fresh = new Progress.Cursor();
          fresh.seqNo = Long.MIN_VALUE;
          return fresh;
        });
        long last = seqNo(page.get(page.size() - 1));
        if (last != cursor.seqNo) {
          cursor.ids.clear();
        }
        cursor.seqNo = last;
        for (Hit hit : page) {
          if (seqNo(hit) == last) {
            cursor.ids.add(hit.id());
          }
        }
        if (wrote) {
          unsaved[0]++;
        }
        boolean due = System.nanoTime() - savedAt[0] >= saveInterval.toNanos();
        if (unsaved[0] >= savePages || due) {
          save.accept(progress);
          unsaved[0] = 0;
          savedAt[0] = System.nanoTime();
        }
      });
    } finally {
      try {
        engine.closePit(pit);
      } catch (RuntimeException e) {
        // it expires on its own
      }
    }
    if (pass != null) {
      progress.finish(pass);
      save.accept(progress);
    }
  }

  /** Pages through an index under a point in time of its own. */
  private void scan(String index, Map<String, Object> body, Consumer<List<Hit>> pages) {
    Pit pit = engine.openPit(List.of(index), keepAlive);
    try {
      scanPit(pit.id(), body, false, sort, pages);
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
    scanPit(pitId, body, snapshot, sort, pages);
  }

  private void scanPit(String pitId, Map<String, Object> body, boolean snapshot,
      List<Object> order, Consumer<List<Hit>> pages) {
    List<Object> after = null;
    while (true) {
      Map<String, Object> request = new LinkedHashMap<>(body);
      request.put("pit", pit(pitId));
      request.put("size", PAGE);
      request.put("sort", order);
      request.put("track_total_hits", false);
      if (after != null) {
        request.put("search_after", after);
      }
      SearchResult result = snapshot ? searchPit(request) : search(List.of(), request);
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
      SearchResult result = engine.search(List.of(), body);
      if (!result.partial()) {
        return result;
      }
      // only a reader that is gone (its node restarted, say) loses the snapshot; any other shard
      // failure (a full search queue, a relocating shard) is transient: the run fails and the next
      // one tries the same snapshot again, never the degraded path that cannot tell deletes apart
      if (!result.shardFailures().isEmpty() && result.shardFailures().stream()
          .allMatch(CONTEXT_MISSING::equals)) {
        throw new SnapshotLost("the point in time over the new index lost its reader on "
            + result.shardsFailed() + " shards (" + CONTEXT_MISSING + ")");
      }
      throw new MigrationException("a search of the snapshot failed on " + result.shardsFailed()
          + " shards " + result.shardFailures() + "; the migration does not act on partial"
          + " results, and the next run tries the snapshot again");
    } catch (EngineException e) {
      if (e.getKind() == EngineException.Kind.NOT_FOUND) {
        throw new SnapshotLost(e);
      }
      throw e;
    }
  }

  /**
   * Searches and fails when a shard did not answer: the engines return 200 with the other shards'
   * hits and an exact-looking total, which the reconcile must never take for the whole index.
   */
  SearchResult search(List<String> indices, Map<String, Object> body) {
    return strict(engine, indices, body);
  }

  /** Searches with {@code engine} and fails on a partial answer; see {@link #search}. */
  static SearchResult strict(SearchEngine engine, List<String> indices, Map<String, Object> body) {
    SearchResult result = engine.search(indices, body);
    if (result.partial()) {
      throw new MigrationException("a search of " + (indices.isEmpty() ? "a point in time"
          : String.join(",", indices)) + " failed on " + result.shardsFailed() + " shards;"
          + " the migration does not act on partial results");
    }
    return result;
  }

  /** Counts the documents of an index exactly, failing on a partial answer. */
  static long count(SearchEngine engine, String index, Map<String, Object> query) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("size", 0);
    body.put("track_total_hits", true);
    if (query != null) {
      body.put("query", query);
    }
    SearchResult result = strict(engine, List.of(index), body);
    if (!result.totalExact()) {
      throw new MigrationException("the engine counted " + index + " only as a lower bound");
    }
    return result.total();
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
