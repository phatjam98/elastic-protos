package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.engine.EngineInfo;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.migration.MigrationResult;
import com.phatjam98.elasticprotos.util.Json;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Soak tool for the migration paths: sustained writes through the alias during a zero-downtime,
 * blocking and backfill migration, a migrating JVM killed at every phase and resumed, and several
 * JVMs bootstrapping at once. Runs against an already running cluster and writes a JSON report per
 * scenario.
 *
 * <pre>{@code
 * ./gradlew :elastic-protos-testkit:soak --args="elasticsearch http://localhost:9200 live"
 * ./gradlew :elastic-protos-testkit:soak --args="opensearch http://localhost:9200 all --docs 50000"
 * }</pre>
 *
 * <p>Exit code 0 when no genuine violation was found, 1 when one was, 2 on a usage error and 3 when
 * the harness itself failed. See {@code docs/soak.md}.
 */
public final class Soak {

  private Soak() {}

  /**
   * Runs the tool.
   *
   * @param args see {@code docs/soak.md}
   */
  public static void main(String[] args) {
    PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8);
    SoakOptions options;
    try {
      options = SoakOptions.parse(args);
    } catch (IllegalArgumentException e) {
      out.println(e.getMessage());
      out.println(SoakOptions.USAGE);
      System.exit(2);
      return;
    }
    int code;
    try (SearchEngine engine = Cluster.connect(options.engine, options.url)) {
      code = run(engine, options, out);
    } catch (Exception e) {
      e.printStackTrace(out);
      code = 3;
    }
    System.exit(code);
  }

  private static int run(SearchEngine engine, SoakOptions options, PrintStream out)
      throws Exception {
    EngineInfo info = engine.info();
    final String label = options.label != null ? options.label
        : (info.product().name() + "-" + info.version()).toLowerCase(Locale.ROOT);
    if (options.expectVersion != null && !options.expectVersion.equals(info.version())) {
      out.println("refusing to run: " + options.url + " reports " + info
          + ", expected version " + options.expectVersion);
      return 2;
    }
    Files.createDirectories(options.out);
    out.println("soak against " + info + " (" + options.url + "), reports in "
        + options.out.toAbsolutePath());
    List<String> scenarios = "all".equals(options.scenario)
        ? List.of("live", "kill", "bootstrap") : List.of(options.scenario);
    long violations = 0;
    boolean harnessError = false;
    for (String scenario : scenarios) {
      Map<String, Object> report = new LinkedHashMap<>();
      report.put("scenario", scenario);
      report.put("engine", Map.of("product", info.product().name(), "version",
          info.version().toString(), "url", options.url));
      report.put("startedAt", Instant.now().toString());
      Map<String, Object> settings = new LinkedHashMap<>();
      settings.put("docs", options.docs(scenario));
      settings.put("writers", options.writers);
      settings.put("ratePerWriter", options.rate);
      settings.put("seed", options.seed);
      settings.put("shards", options.shards);
      report.put("settings", settings);
      List<Object> runs = new ArrayList<>();
      long found = 0;
      String verdict;
      try {
        switch (scenario) {
          case "live":
            found = LiveScenario.run(engine, options, out, runs);
            break;
          case "kill":
            settings.put("lockTtlMs", options.lockTtl.toMillis());
            found = KillScenario.run(engine, options, label, out, runs);
            break;
          default:
            settings.put("jvms", options.jvms);
            settings.put("lockTtlMs", options.lockTtl.toMillis());
            found = BootstrapScenario.run(engine, options, label, out, runs);
            break;
        }
        verdict = found == 0 ? "PASS" : "VIOLATIONS";
      } catch (Exception e) {
        // keep the runs that finished, and go on with the other scenarios
        e.printStackTrace(out);
        harnessError = true;
        verdict = "HARNESS_ERROR";
        report.put("harnessError", String.valueOf(e));
      }
      report.put("finishedAt", Instant.now().toString());
      report.put("violations", found);
      report.put("verdict", verdict);
      report.put("runs", runs);
      Path file = options.out.resolve(label + "-" + scenario + ".json");
      write(file, report);
      out.println();
      out.println("== " + scenario + ": " + verdict + (found == 0 ? "" : ", " + found
          + " VIOLATION(S)") + " -> " + file.toAbsolutePath());
      violations += found;
    }
    return harnessError ? 3 : violations == 0 ? 0 : 1;
  }

  private static void write(Path file, Map<String, Object> report) throws IOException {
    Files.writeString(file, Json.pretty(report) + "\n", StandardCharsets.UTF_8);
  }

  /** Describes an outcome for a report. */
  static Map<String, Object> outcome(MigrationResult.Outcome outcome) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("alias", outcome.plan().alias());
    map.put("action", outcome.plan().action().name());
    map.put("status", outcome.status().name());
    map.put("index", outcome.index());
    map.put("retained", outcome.retainedIndices());
    map.put("sourceCount", outcome.sourceCount());
    map.put("destCount", outcome.destCount());
    if (outcome.reconcile() != null) {
      Map<String, Object> reconcile = new LinkedHashMap<>();
      reconcile.put("copied", outcome.reconcile().copied());
      reconcile.put("superseded", outcome.reconcile().superseded());
      reconcile.put("ghostsDeleted", outcome.reconcile().ghostsDeleted());
      reconcile.put("staleSkipped", outcome.reconcile().staleSkipped());
      map.put("reconcile", reconcile);
    }
    map.put("warnings", outcome.warnings());
    map.put("error", outcome.error() == null ? null : String.valueOf(outcome.error()));
    map.put("reason", outcome.plan().reason());
    return map;
  }

  /** Sleeps, restoring the interrupt flag. */
  static void pause(java.time.Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted", e);
    }
  }
}
