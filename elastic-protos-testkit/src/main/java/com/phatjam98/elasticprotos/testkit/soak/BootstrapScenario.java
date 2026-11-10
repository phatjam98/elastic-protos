package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.mapping.ProtoMapper;
import com.phatjam98.elasticprotos.migration.IndexMigrator;
import com.phatjam98.elasticprotos.migration.IndexPlan;
import com.phatjam98.elasticprotos.migration.MigrationResult;
import com.phatjam98.elasticprotos.proto.example.Document;
import com.phatjam98.elasticprotos.proto.example.Firm;
import com.phatjam98.elasticprotos.proto.example.InboxItem;
import com.phatjam98.elasticprotos.proto.example.TypedMaps;
import com.phatjam98.elasticprotos.util.Json;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Scenario 3: K JVMs bootstrap the same aliases at the same instant, against an empty cluster
 * ({@code create}) and against aliases whose mapping changed ({@code reindex}, the documents alias
 * seeded), each with the default {@code lockWait} and with {@code lockWait} zero; and against an
 * interrupted zero-downtime reindex ({@code resume}: a child was killed after the swap, leaving
 * the pending marker and its lock), with the default {@code lockWait}. Exactly one JVM
 * migrates each alias; the others end {@code SKIPPED} (planned again under the lock as {@code
 * NO_OP}) or, with {@code lockWait} zero, {@code LOCKED}.
 */
final class BootstrapScenario {

  private BootstrapScenario() {}

  static long run(SearchEngine engine, SoakOptions options, String label, PrintStream out,
      List<Object> runs) throws Exception {
    long violations = 0;
    for (String kind : List.of("create", "reindex", "resume")) {
      // a resume waits for the dead holder's lock to expire, so only with the default lockWait
      for (long lockWait : "resume".equals(kind) ? List.of(600_000L) : List.of(600_000L, 0L)) {
        Map<String, Object> report = runOne(engine, options, label, out, kind, lockWait);
        runs.add(report);
        violations += (Long) report.get("violations");
      }
    }
    return violations;
  }

  /**
   * Leaves an interrupted zero-downtime reindex of the documents alias behind: a child migrates it
   * and is killed after the swap, with the pending marker on the new index and its lock held.
   */
  private static void interrupt(SoakOptions options, String label, PrintStream out,
      String prefix, String name) throws Exception {
    java.util.concurrent.CountDownLatch held = new java.util.concurrent.CountDownLatch(1);
    Child child = Child.start(SoakWorker.command(options.engine, options.url, "migrate",
        List.of("prefix=" + prefix, "version=reindex", "killAt=SWAPPED",
            "lockTtlMs=" + options.lockTtl.toMillis())),
        options.out.resolve(label + "-bootstrap-" + name + "-killed.log"),
        line -> {
          if (line.startsWith("HOLD ")) {
            held.countDown();
          }
        });
    if (!held.await(10, java.util.concurrent.TimeUnit.MINUTES)) {
      child.kill();
      throw new IllegalStateException("the migrating child never reached SWAPPED: "
          + child.tail());
    }
    child.kill();
    out.println("  killed the migrating child after SWAPPED; the reindex is pending");
  }

  private static Map<String, Object> runOne(SearchEngine engine, SoakOptions options,
      String label, PrintStream out, String kind, long lockWait) throws Exception {
    String name = kind + (lockWait == 0 ? "-nowait" : "-wait");
    String prefix = "soak-boot-" + name;
    int jvms = options.jvms;
    out.println();
    out.println("== bootstrap " + name + ": " + jvms + " JVMs at once");
    Cluster.clean(engine, prefix);
    ProtoMapper base = SoakSchema.mapper("base", prefix);
    OpLog seeds = new OpLog();
    int docs = 0;
    String documents = base.map(Document.class).alias();
    if (!"create".equals(kind)) {
      MigrationResult created = IndexMigrator.builder().engine(engine).mapper(base).build()
          .bootstrap(Document.class, Firm.class, InboxItem.class, TypedMaps.class);
      if (!created.ok()) {
        throw new IllegalStateException("bootstrap failed: " + created.problems());
      }
      docs = options.docs("bootstrap");
      Cluster.seed(engine, documents, docs, seeds, options.seed);
    }
    if ("resume".equals(kind)) {
      interrupt(options, label, out, prefix, name);
    }
    String version = "create".equals(kind) ? "base" : "reindex";
    ProtoMapper target = SoakSchema.mapper(version, prefix);
    Map<String, String> expected = new TreeMap<>();
    for (IndexPlan item : IndexMigrator.builder().engine(engine).mapper(target).build()
        .plan(Document.class, Firm.class, InboxItem.class, TypedMaps.class).items()) {
      expected.put(item.alias(), item.action().name());
    }
    Map<String, String> before = new TreeMap<>();
    for (String alias : expected.keySet()) {
      Set<String> holders = Cluster.holders(engine, alias);
      before.put(alias, holders.isEmpty() ? null : holders.iterator().next());
    }

    Map<String, Object> pendingBefore = before.get(documents) == null ? Map.of()
        : Cluster.pending(engine, before.get(documents));

    long startAt = System.currentTimeMillis() + 8000 + 300L * jvms;
    List<Child> children = new ArrayList<>();
    List<Map<String, Object>> results = new ArrayList<>();
    for (int k = 0; k < jvms; k++) {
      List<String> params = List.of("prefix=" + prefix, "version=" + version,
          "startAt=" + startAt, "lockWaitMs=" + lockWait,
          "lockTtlMs=" + options.lockTtl.toMillis());
      children.add(Child.start(SoakWorker.command(options.engine, options.url, "bootstrap",
          params), options.out.resolve(label + "-bootstrap-" + name + "-" + k + ".log"),
          line -> {
            if (line.startsWith("RESULT ")) {
              synchronized (results) {
                results.add(Json.parseObject(line.substring(7)));
              }
            }
          }));
    }
    List<Integer> exits = new ArrayList<>();
    for (Child child : children) {
      Integer code = child.await(1200);
      if (code == null) {
        child.kill();
      }
      exits.add(code);
    }

    List<String> checks = new ArrayList<>();
    if ("resume".equals(kind) && pendingBefore.isEmpty()) {
      checks.add("the interrupted reindex left no pending marker on " + before.get(documents));
    }
    if (results.size() != jvms) {
      checks.add("only " + results.size() + " of " + jvms + " JVMs reported a result; exits "
          + exits);
    }
    long firstStart = Long.MAX_VALUE;
    long lastStart = Long.MIN_VALUE;
    Map<String, Map<String, Long>> statuses = new TreeMap<>();
    for (Map<String, Object> result : results) {
      long started = ((Number) result.get("startedAt")).longValue();
      firstStart = Math.min(firstStart, started);
      lastStart = Math.max(lastStart, started);
      for (Object o : (List<?>) result.get("outcomes")) {
        Map<String, Object> outcome = Cluster.map(o);
        statuses.computeIfAbsent(String.valueOf(outcome.get("alias")), a -> new TreeMap<>())
            .merge(outcome.get("action") + "/" + outcome.get("status"), 1L, Long::sum);
      }
    }
    Map<String, Object> aliases = new TreeMap<>();
    for (Map.Entry<String, String> entry : expected.entrySet()) {
      String alias = entry.getKey();
      boolean changes = !"NO_OP".equals(entry.getValue());
      Map<String, Long> counts = statuses.getOrDefault(alias, Map.of());
      long applied = 0;
      long other = 0;
      for (Map.Entry<String, Long> count : counts.entrySet()) {
        String status = count.getKey().substring(count.getKey().indexOf('/') + 1);
        if ("APPLIED".equals(status)) {
          applied += count.getValue();
        } else if (!"SKIPPED".equals(status) && !("LOCKED".equals(status) && lockWait == 0)) {
          other += count.getValue();
        }
      }
      if (applied != (changes ? 1 : 0)) {
        checks.add(alias + ": " + applied + " JVMs applied " + entry.getValue() + ", expected "
            + (changes ? 1 : 0) + " (" + counts + ")");
      }
      if (other > 0) {
        checks.add(alias + ": unexpected outcomes " + counts);
      }
      Set<String> holders = Cluster.holders(engine, alias);
      if (holders.size() != 1) {
        checks.add(alias + " points at " + holders);
      }
      List<String> indices = new ArrayList<>();
      for (String index : Cluster.indices(engine, prefix).keySet()) {
        if (index.startsWith(alias + "-v")) {
          indices.add(index);
        }
      }
      int wanted = "REINDEX".equals(entry.getValue()) ? 2 : 1;
      if (indices.size() != wanted) {
        checks.add(alias + " has " + indices.size() + " indices, expected " + wanted + ": "
            + indices);
      }
      for (String holder : holders) {
        Map<String, Object> pending = Cluster.pending(engine, holder);
        if (!pending.isEmpty()) {
          checks.add(holder + " carries a pending marker " + pending);
        }
      }
      Map<String, Object> summary = new LinkedHashMap<>();
      summary.put("planned", entry.getValue());
      summary.put("before", before.get(alias));
      summary.put("holders", holders);
      summary.put("indices", indices);
      summary.put("outcomes", counts);
      aliases.put(alias, summary);
    }
    long locks = Cluster.lockCount(engine, prefix);
    if (locks != 0) {
      checks.add("lock index holds " + locks + " documents");
    }
    Map<String, Object> report = new LinkedHashMap<>();
    report.put("run", name);
    report.put("jvms", jvms);
    report.put("lockWaitMs", lockWait);
    report.put("pendingBefore", pendingBefore);
    report.put("exitCodes", exits);
    report.put("startSpreadMs", results.isEmpty() ? null : lastStart - firstStart);
    report.put("aliases", aliases);
    report.put("lockDocumentsLeft", locks);
    if (docs > 0) {
      LogComparison.Result comparison = LogComparison.compare(seeds.snapshot(), Set.of(),
          Cluster.scan(engine, documents), LogComparison.Timeline.strict(""), 10);
      report.put("documentsComparison", comparison.toMap());
      if (comparison.violationCount() > 0) {
        checks.add(documents + ": " + comparison.violationCount()
            + " documents differ from the seed");
      }
    }
    report.put("results", results);
    report.put("checks", checks);
    report.put("violations", (long) checks.size());
    Summary.printBootstrap(out, report);
    if (!options.keep) {
      Cluster.clean(engine, prefix);
    }
    return report;
  }
}
