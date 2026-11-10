package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.migration.IndexMigrator;
import com.phatjam98.elasticprotos.migration.MigrationOptions;
import com.phatjam98.elasticprotos.migration.MigrationPlan;
import com.phatjam98.elasticprotos.migration.MigrationResult;
import com.phatjam98.elasticprotos.proto.example.Document;
import com.phatjam98.elasticprotos.proto.example.Firm;
import com.phatjam98.elasticprotos.proto.example.InboxItem;
import com.phatjam98.elasticprotos.proto.example.TypedMaps;
import com.phatjam98.elasticprotos.util.Json;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The child JVM of the kill and bootstrap scenarios. It talks to the parent through lines on
 * standard output that start with {@code SOAK }:
 *
 * <ul>
 *   <li>{@code SOAK PHASE <phase> <epoch millis>}: the migration reported a phase;
 *   <li>{@code SOAK HOLD <phase>}: the kill phase was reached; the migrating thread now sleeps
 *       inside the listener (holding the alias lock, which its heartbeat keeps renewing) until the
 *       parent kills the process;
 *   <li>{@code SOAK START <epoch millis>}: the bootstrap starts now;
 *   <li>{@code SOAK RESULT <json>}: the outcomes.
 * </ul>
 *
 * <pre>
 * SoakWorker &lt;engine&gt; &lt;url&gt; migrate prefix=P version=reindex|backfill killAt=PHASE
 *     lockTtlMs=N [swapGraceMs=N]
 * SoakWorker &lt;engine&gt; &lt;url&gt; bootstrap prefix=P version=base|reindex startAt=MILLIS
 *     lockWaitMs=N lockTtlMs=N
 * </pre>
 */
public final class SoakWorker {

  private static final PrintStream OUT =
      new PrintStream(System.out, true, StandardCharsets.UTF_8);

  private SoakWorker() {}

  /**
   * Runs the child.
   *
   * @param args see the class comment
   * @throws Exception when the cluster cannot be reached
   */
  public static void main(String[] args) throws Exception {
    Map<String, String> params = new HashMap<>();
    for (int i = 3; i < args.length; i++) {
      int eq = args[i].indexOf('=');
      params.put(args[i].substring(0, eq), args[i].substring(eq + 1));
    }
    try (SearchEngine engine = Cluster.connect(args[0], args[1])) {
      if ("migrate".equals(args[2])) {
        migrate(engine, params);
      } else {
        bootstrap(engine, params);
      }
    }
    System.exit(0);
  }

  private static synchronized void send(String line) {
    OUT.println("SOAK " + line);
    OUT.flush();
  }

  private static void migrate(SearchEngine engine, Map<String, String> params) {
    String prefix = params.get("prefix");
    String killAt = params.get("killAt");
    MigrationOptions.Builder options = MigrationOptions.builder()
        .lockTtl(Duration.ofMillis(Long.parseLong(params.get("lockTtlMs"))));
    if (params.containsKey("swapGraceMs")) {
      options.swapGrace(Duration.ofMillis(Long.parseLong(params.get("swapGraceMs"))));
    }
    IndexMigrator migrator = IndexMigrator.builder().engine(engine)
        .mapper(SoakSchema.mapper(params.get("version"), prefix))
        .options(options.build())
        .listener((alias, phase) -> {
          send("PHASE " + phase + " " + System.currentTimeMillis());
          if (phase.name().equals(killAt)) {
            send("HOLD " + phase);
            while (true) {
              try {
                Thread.sleep(60_000);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
            }
          }
        })
        .build();
    MigrationResult result = migrator.bootstrap(Document.class);
    send("RESULT " + Json.canonical(outcomes(result)));
  }

  private static void bootstrap(SearchEngine engine, Map<String, String> params)
      throws InterruptedException {
    String prefix = params.get("prefix");
    MigrationOptions options = MigrationOptions.builder()
        .lockWait(Duration.ofMillis(Long.parseLong(params.get("lockWaitMs"))))
        .lockTtl(Duration.ofMillis(Long.parseLong(params.get("lockTtlMs"))))
        .build();
    final IndexMigrator migrator = IndexMigrator.builder().engine(engine)
        .mapper(SoakSchema.mapper(params.get("version"), prefix))
        .options(options)
        .build();
    engine.info();
    long startAt = Long.parseLong(params.get("startAt"));
    long wait = startAt - System.currentTimeMillis();
    if (wait > 0) {
      Thread.sleep(wait);
    }
    long started = System.currentTimeMillis();
    send("START " + started);
    MigrationPlan plan = migrator.plan(Document.class, Firm.class, InboxItem.class,
        TypedMaps.class);
    MigrationResult result = migrator.apply(plan);
    Map<String, Object> report = new LinkedHashMap<>();
    report.put("pid", ProcessHandle.current().pid());
    report.put("startedAt", started);
    report.put("finishedAt", System.currentTimeMillis());
    report.put("outcomes", outcomes(result));
    send("RESULT " + Json.canonical(report));
  }

  private static List<Object> outcomes(MigrationResult result) {
    List<Object> outcomes = new ArrayList<>();
    for (MigrationResult.Outcome outcome : result.outcomes()) {
      outcomes.add(Soak.outcome(outcome));
    }
    return outcomes;
  }

  /**
   * Returns the command that starts a worker with the parent's classpath.
   *
   * @param engine the engine product
   * @param url the cluster URL
   * @param command {@code migrate} or {@code bootstrap}
   * @param params {@code key=value} parameters
   * @return the command line
   */
  static List<String> command(String engine, String url, String command, List<String> params) {
    String java = ProcessHandle.current().info().command()
        .orElse(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    List<String> line = new ArrayList<>(List.of(java, "-Xmx512m", "-cp",
        System.getProperty("java.class.path"), SoakWorker.class.getName(), engine, url, command));
    line.addAll(params);
    return line;
  }
}
