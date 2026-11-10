package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.engine.Hit;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.mapping.ProtoMapper;
import com.phatjam98.elasticprotos.migration.Action;
import com.phatjam98.elasticprotos.migration.IndexMigrator;
import com.phatjam98.elasticprotos.migration.IndexPlan;
import com.phatjam98.elasticprotos.migration.MigrationOptions;
import com.phatjam98.elasticprotos.migration.MigrationPlan;
import com.phatjam98.elasticprotos.migration.MigrationResult;
import com.phatjam98.elasticprotos.proto.example.Document;
import com.phatjam98.elasticprotos.testkit.soak.WriteFailures.Mode;
import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Scenario 2: for each phase, a child JVM migrates the alias while the parent's writers run, and
 * is killed with {@code SIGKILL} once it reports that phase. A fresh migrator in the parent then
 * resumes; the run checks that it completes, clears the pending marker, takes the dead holder's
 * lock over in time, and that the alias matches the operation log.
 */
final class KillScenario {

  private KillScenario() {}

  static long run(SearchEngine engine, SoakOptions options, String label, PrintStream out,
      List<Object> runs) throws Exception {
    long violations = 0;
    for (String phase : options.phases) {
      Map<String, Object> report = runPhase(engine, options, label, out, phase);
      runs.add(report);
      violations += (Long) report.get("violations");
    }
    return violations;
  }

  private static Map<String, Object> runPhase(SearchEngine engine, SoakOptions options,
      String label, PrintStream out, String killAt) throws Exception {
    boolean backfill = killAt.startsWith("BACKFILL");
    final Mode mode = backfill ? Mode.BACKFILL : Mode.ZERO_DOWNTIME;
    final String version = backfill ? "backfill" : "reindex";
    String prefix = "soak-kill-" + killAt.toLowerCase(Locale.ROOT).replace('_', '-');
    int docs = options.docs("kill");
    out.println();
    out.println("== kill at " + killAt + ": " + docs + " documents, lockTtl " + options.lockTtl);
    Cluster.clean(engine, prefix);
    ProtoMapper base = SoakSchema.mapper("base", prefix);
    MigrationResult created = IndexMigrator.builder().engine(engine).mapper(base).build()
        .bootstrap(Document.class);
    if (!created.ok()) {
      throw new IllegalStateException("bootstrap failed: " + created.problems());
    }
    String alias = base.map(Document.class).alias();
    String oldIndex = Cluster.holder(engine, alias);
    LoadRun load = new LoadRun(engine, alias, oldIndex, options, docs);
    load.seed(options.seed);
    load.start();
    Soak.pause(options.warmup);

    final List<String> checks = new ArrayList<>();
    Map<String, Object> report = new LinkedHashMap<>();
    report.put("killAt", killAt);
    report.put("alias", alias);
    report.put("oldIndex", oldIndex);

    long origin = System.currentTimeMillis();
    load.clock.mark(PhaseClock.STARTED, origin, "parent");
    CountDownLatch held = new CountDownLatch(1);
    List<String> params = new ArrayList<>(List.of("prefix=" + prefix, "version=" + version,
        "killAt=" + killAt, "lockTtlMs=" + options.lockTtl.toMillis()));
    if (options.swapGrace != null) {
      params.add("swapGraceMs=" + options.swapGrace.toMillis());
    }
    Child child = Child.start(
        SoakWorker.command(options.engine, options.url, "migrate", params),
        options.out.resolve(label + "-kill-" + killAt + "-child.log"),
        line -> {
          String[] parts = line.split(" ");
          if ("PHASE".equals(parts[0])) {
            load.clock.mark(parts[1], Long.parseLong(parts[2]), "child");
            out.println("  child phase " + parts[1]);
          } else if ("HOLD".equals(parts[0])) {
            held.countDown();
          }
        });
    boolean reached = held.await(30, TimeUnit.MINUTES);
    final Optional<Hit> lockAtKill =
        reached ? Cluster.lock(engine, prefix, alias) : Optional.empty();
    child.kill();
    long killedAt = System.currentTimeMillis();
    load.clock.mark("KILLED", killedAt, "parent");
    out.println("  killed child " + child.pid() + (reached ? " at " + killAt
        : " (it never reached " + killAt + "; last output " + child.tail() + ")"));
    if (!reached) {
      checks.add("the child never reached " + killAt + ": " + child.tail());
    }
    final String childHolder = lockAtKill.map(h -> String.valueOf(h.source().get("holder")))
        .orElse(null);
    final String expiresAt = lockAtKill.map(h -> String.valueOf(h.source().get("expires_at")))
        .orElse(null);
    Map<String, Object> afterKill = new LinkedHashMap<>();
    afterKill.put("aliasHolders", Cluster.holders(engine, alias));
    afterKill.put("indices", Cluster.indices(engine, prefix).keySet());
    for (String index : Cluster.holders(engine, alias)) {
      afterKill.put("pendingOnHolder", Cluster.pending(engine, index));
    }
    afterKill.put("lockHolder", childHolder);
    afterKill.put("lockExpiresAt", expiresAt);
    report.put("afterKill", afterKill);

    Soak.pause(options.afterKill);
    AtomicLong freedAt = new AtomicLong(-1);
    AtomicLong takenAt = new AtomicLong(-1);
    AtomicReference<String> taker = new AtomicReference<>();
    Thread poller = new Thread(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Optional<Hit> lock = Cluster.lock(engine, prefix, alias);
          String now = lock.map(h -> String.valueOf(h.source().get("holder"))).orElse(null);
          long t = System.currentTimeMillis();
          if (childHolder != null && !childHolder.equals(now)) {
            freedAt.compareAndSet(-1, t);
            if (now != null && takenAt.compareAndSet(-1, t)) {
              taker.set(now);
            }
          }
          Thread.sleep(100);
        } catch (InterruptedException e) {
          return;
        } catch (RuntimeException e) {
          // keep polling
        }
      }
    }, "soak-lock-poller");
    poller.setDaemon(true);
    poller.start();

    MigrationOptions.Builder migration = MigrationOptions.builder()
        .lockTtl(options.lockTtl).lockWait(Duration.ofMinutes(10));
    if (options.swapGrace != null) {
      migration.swapGrace(options.swapGrace);
    }
    IndexMigrator migrator = IndexMigrator.builder().engine(engine)
        .mapper(SoakSchema.mapper(version, prefix))
        .options(migration.build())
        .listener((a, phase) -> {
          load.clock.mark(phase.name(), System.currentTimeMillis(), "resume");
          out.println("  resumed phase " + phase);
        })
        .build();
    MigrationPlan plan = migrator.plan(Document.class);
    IndexPlan item = plan.items().get(0);
    Map<String, Object> resumePlan = new LinkedHashMap<>();
    resumePlan.put("action", item.action().name());
    resumePlan.put("reason", item.reason());
    resumePlan.put("resume", item.resume() == null ? null : String.valueOf(item.resume()));
    report.put("resumePlan", resumePlan);
    long resumeStarted = System.currentTimeMillis();
    MigrationResult result = migrator.apply(plan);
    long resumeMs = System.currentTimeMillis() - resumeStarted;
    poller.interrupt();
    poller.join();
    MigrationResult.Outcome outcome = result.outcomes().get(0);
    out.println("  resume " + item.action() + " -> " + outcome.status() + " in " + resumeMs
        + " ms");
    report.put("resumeOutcome", Soak.outcome(outcome));
    report.put("resumeMs", resumeMs);
    if (outcome.status() != MigrationResult.Status.APPLIED
        && outcome.status() != MigrationResult.Status.SKIPPED) {
      checks.add("the resumed migration ended " + outcome.status() + ": " + outcome.error());
    }

    // the lock: taken over in time when the resume needed it, otherwise left to expire
    Map<String, Object> lock = new LinkedHashMap<>();
    long ttl = options.lockTtl.toMillis();
    lock.put("lockTtlMs", ttl);
    lock.put("childHolder", childHolder);
    lock.put("expiresAtAfterKillMs", expiresAt == null ? null
        : Instant.parse(expiresAt).toEpochMilli() - killedAt);
    lock.put("freedAfterKillMs", freedAt.get() < 0 ? null : freedAt.get() - killedAt);
    lock.put("takenOverAfterKillMs", takenAt.get() < 0 ? null : takenAt.get() - killedAt);
    lock.put("takenOverBy", taker.get());
    Optional<Hit> left = Cluster.lock(engine, prefix, alias);
    lock.put("leftHolder", left.map(h -> h.source().get("holder")).orElse(null));
    if (expiresAt != null && childHolder != null) {
      long expires = Instant.parse(expiresAt).toEpochMilli();
      if (expires - killedAt > ttl + 1000) {
        checks.add("the lock at the kill expires " + (expires - killedAt) + " ms after it, more"
            + " than lockTtl");
      }
      // documented: taken over a third of the TTL after it expired; the waiter polls every 1-2 s
      long bound = expires - killedAt + ttl / 3 + 3000;
      lock.put("takeoverBoundMs", bound);
      if (item.action() != Action.NO_OP) {
        if (takenAt.get() < 0) {
          checks.add("the resumed migration never showed up as the lock holder");
        } else if (takenAt.get() - killedAt > bound) {
          checks.add("the lock was taken over " + (takenAt.get() - killedAt) + " ms after the"
              + " kill, later than the documented bound of " + bound + " ms");
        }
        if (left.isPresent()) {
          checks.add("the lock document is still there after the resume: " + left.get().source());
        }
      } else if (left.isPresent() && !childHolder.equals(left.get().source().get("holder"))) {
        checks.add("unexpected lock holder after a NO_OP resume: " + left.get().source());
      }
      lock.put("staleLockLeftToExpire", item.action() == Action.NO_OP && left.isPresent());
    }
    report.put("lock", lock);

    load.clock.mark(PhaseClock.AFTER);
    Soak.pause(options.post);
    load.stop();

    String holder = Cluster.holder(engine, alias);
    report.put("newIndex", holder);
    Map<String, Object> pending = Cluster.pending(engine, holder);
    if (!pending.isEmpty()) {
      checks.add(holder + " still carries a pending marker: " + pending);
    }
    if (!backfill && holder.equals(oldIndex)) {
      checks.add("the alias still points at the old index " + oldIndex);
    }
    IndexPlan again = migrator.plan(Document.class).items().get(0);
    if (again.action() != Action.NO_OP) {
      checks.add("planning again gives " + again.action() + ": " + again.reason());
    }
    report.put("indices", Cluster.indices(engine, prefix).keySet());

    Map<String, Object> verified = load.verify(mode, origin);
    report.put("checks", checks);
    report.putAll(verified);
    long violations = (Long) verified.get("violations") + checks.size();
    report.put("violations", violations);
    Summary.printKill(out, report);
    if (!options.keep) {
      Cluster.clean(engine, prefix);
    }
    return report;
  }
}
