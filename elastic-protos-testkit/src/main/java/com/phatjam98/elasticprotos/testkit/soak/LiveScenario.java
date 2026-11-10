package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.mapping.ProtoMapper;
import com.phatjam98.elasticprotos.migration.Action;
import com.phatjam98.elasticprotos.migration.IndexMigrator;
import com.phatjam98.elasticprotos.migration.IndexPlan;
import com.phatjam98.elasticprotos.migration.MigrationOptions;
import com.phatjam98.elasticprotos.migration.MigrationPlan;
import com.phatjam98.elasticprotos.migration.MigrationResult;
import com.phatjam98.elasticprotos.migration.ReindexMode;
import com.phatjam98.elasticprotos.proto.example.Document;
import com.phatjam98.elasticprotos.testkit.soak.WriteFailures.Mode;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Scenario 1: seeds documents, starts writers and a stale reader, migrates the alias while they
 * run (a zero-downtime reindex, a blocking reindex, a backfill, a zero-downtime reindex through a
 * migration script) and compares the alias with the operation log.
 */
final class LiveScenario {

  private LiveScenario() {}

  static long run(SearchEngine engine, SoakOptions options, PrintStream out, List<Object> runs)
      throws Exception {
    long violations = 0;
    for (String mode : options.modes) {
      Map<String, Object> report = runMode(engine, options, out, mode);
      runs.add(report);
      violations += (Long) report.get("violations");
    }
    return violations;
  }

  private static Map<String, Object> runMode(SearchEngine engine, SoakOptions options,
      PrintStream out, String modeName) throws Exception {
    Mode mode;
    String version;
    switch (modeName) {
      case "zero-downtime":
        mode = Mode.ZERO_DOWNTIME;
        version = "reindex";
        break;
      case "scripted":
        mode = Mode.ZERO_DOWNTIME;
        version = "scripted";
        break;
      case "blocking":
        mode = Mode.BLOCKING;
        version = "reindex";
        break;
      case "backfill":
        mode = Mode.BACKFILL;
        version = "backfill";
        break;
      default:
        throw new IllegalArgumentException("unknown mode " + modeName);
    }
    String prefix = "soak-live-" + modeName;
    int docs = options.docs("live");
    out.println();
    out.println("== live " + modeName + ": " + docs + " documents, " + options.shards
        + " shards, " + options.writers + " writers at " + options.rate + " ops/s each");
    Cluster.clean(engine, prefix);
    ProtoMapper base = SoakSchema.mapper("base", prefix, options.shards);
    MigrationResult created = IndexMigrator.builder().engine(engine).mapper(base).build()
        .bootstrap(Document.class);
    if (!created.ok()) {
      throw new IllegalStateException("bootstrap failed: " + created.problems());
    }
    String alias = base.map(Document.class).alias();
    String oldIndex = Cluster.holder(engine, alias);
    LoadRun load = new LoadRun(engine, alias, oldIndex, options, docs);
    load.seed(options.seed);
    out.println("seeded " + docs + " documents into " + oldIndex);

    MigrationOptions.Builder migration = MigrationOptions.builder()
        .reindexMode(mode == Mode.BLOCKING ? ReindexMode.BLOCKING : ReindexMode.ZERO_DOWNTIME);
    if (options.swapGrace != null) {
      migration.swapGrace(options.swapGrace);
    }
    IndexMigrator migrator = IndexMigrator.builder().engine(engine)
        .mapper(SoakSchema.mapper(version, prefix, options.shards))
        .scriptProvider(SoakSchema.scripts(version))
        .options(migration.build())
        .listener((a, phase) -> {
          load.clock.mark(phase.name());
          out.println("  phase " + phase);
        })
        .build();
    MigrationPlan plan = migrator.plan(Document.class);
    IndexPlan item = plan.items().get(0);
    Action expected = mode == Mode.BACKFILL ? Action.BACKFILL : Action.REINDEX;
    if (item.action() != expected) {
      throw new IllegalStateException("expected " + expected + " but planned " + item.action()
          + ": " + item.reason());
    }

    load.start();
    Soak.pause(options.warmup);
    long origin = System.currentTimeMillis();
    load.clock.mark(PhaseClock.STARTED, origin, "parent");
    MigrationResult result = migrator.apply(plan);
    long migrationMs = System.currentTimeMillis() - origin;
    load.clock.mark(PhaseClock.AFTER);
    out.println("  migration " + result.outcomes().get(0).status() + " in " + migrationMs + " ms");
    Soak.pause(options.post);
    load.stop();

    List<String> checks = new ArrayList<>();
    MigrationResult.Outcome outcome = result.outcomes().get(0);
    if (outcome.status() != MigrationResult.Status.APPLIED) {
      checks.add("migration ended " + outcome.status() + ": " + outcome.error());
    }
    String holder = Cluster.holder(engine, alias);
    checks.addAll(endState(engine, migrator, holder, oldIndex, version,
        mode == Mode.BACKFILL));
    long locks = Cluster.lockCount(engine, prefix);
    if (locks != 0) {
      checks.add("lock index holds " + locks + " documents");
    }

    Map<String, Object> verified = load.verify(mode, origin);
    Map<String, Object> report = new LinkedHashMap<>();
    report.put("mode", modeName);
    report.put("shards", options.shards);
    report.put("alias", alias);
    report.put("oldIndex", oldIndex);
    report.put("newIndex", holder);
    report.put("migrationMs", migrationMs);
    report.put("outcome", Soak.outcome(outcome));
    report.put("checks", checks);
    report.putAll(verified);
    long violations = (Long) verified.get("violations") + checks.size();
    report.put("violations", violations);
    Summary.printLive(out, report);
    if (!options.keep) {
      Cluster.clean(engine, prefix);
    }
    return report;
  }

  /**
   * Checks the state a finished migration must leave: the alias on an index that takes writes,
   * moved off the old index by a reindex (kept by a backfill), without a pending marker, with the
   * target mapping in effect, and planning again {@code NO_OP}.
   *
   * @return the failed checks
   */
  static List<String> endState(SearchEngine engine, IndexMigrator migrator, String holder,
      String oldIndex, String version, boolean inPlace) {
    List<String> checks = new ArrayList<>();
    if (Cluster.writeBlocked(engine, holder)) {
      checks.add("the alias target " + holder + " is write-blocked");
    }
    if (inPlace ? !holder.equals(oldIndex) : holder.equals(oldIndex)) {
      checks.add("the alias points at " + holder + (inPlace ? ", not at " + oldIndex
          : ", still the old index"));
    }
    Map<String, Object> pending = Cluster.pending(engine, holder);
    if (!pending.isEmpty()) {
      checks.add(holder + " still carries a pending marker: " + pending);
    }
    String mapping = SoakSchema.mappingProblem(engine.getMappings(holder), version);
    if (mapping != null) {
      checks.add("the target mapping is not in effect on " + holder + ": " + mapping);
    }
    IndexPlan again = migrator.plan(Document.class).items().get(0);
    if (again.action() != Action.NO_OP) {
      checks.add("planning again gives " + again.action() + ": " + again.reason());
    }
    return checks;
  }
}
