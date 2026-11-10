package com.phatjam98.elasticprotos.testkit.soak;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** The command line of the soak tool. */
public final class SoakOptions {

  /** The zero-downtime phases a kill scenario stops at by default, then the backfill ones. */
  public static final List<String> DEFAULT_PHASES = List.of("PREPARED", "COPIED", "CAUGHT_UP",
      "SNAPSHOT", "SWAPPED", "FROZEN", "RECONCILED", "SWEPT", "FINISHED", "BACKFILL_MARKED",
      "BACKFILLED");

  String engine;
  String url;
  String scenario;
  Integer docs;
  int writers = 4;
  double rate = 50;
  List<String> modes = List.of("zero-downtime", "blocking", "backfill");
  List<String> phases = DEFAULT_PHASES;
  int jvms = 5;
  Duration lockTtl = Duration.ofSeconds(15);
  Duration swapGrace;
  Duration warmup = Duration.ofSeconds(3);
  Duration post = Duration.ofSeconds(5);
  Duration afterKill = Duration.ofSeconds(2);
  Path out = Path.of("build", "soak");
  String label;
  long seed = 42;
  boolean keep;

  /** The usage text. */
  static final String USAGE = String.join("\n",
      "usage: soak <elasticsearch|opensearch> <url> <live|kill|bootstrap|all> [options]",
      "  --docs N           seeded documents (default: live 200000, kill 50000, bootstrap 10000)",
      "  --writers N        writer threads (default 4)",
      "  --rate R           operations per second per writer (default 50)",
      "  --modes a,b        live: zero-downtime,blocking,backfill (default all three)",
      "  --phases A,B       kill: phases to kill at (default " + String.join(",", DEFAULT_PHASES)
          + ")",
      "  --jvms K           bootstrap: concurrent JVMs (default 5)",
      "  --lock-ttl S       kill/bootstrap: lockTtl in seconds (default 15)",
      "  --swap-grace S     swapGrace in seconds (default: the library default, 5)",
      "  --post-seconds S   writes after the migration returned (default 5)",
      "  --out DIR          report directory (default build/soak)",
      "  --label L          report file prefix (default <product>-<version>)",
      "  --seed N           random seed (default 42)",
      "  --keep             keep the soak indices afterwards");

  private SoakOptions() {}

  /**
   * Parses the command line.
   *
   * @param args the arguments
   * @return the options
   * @throws IllegalArgumentException on a usage error
   */
  public static SoakOptions parse(String[] args) {
    if (args.length < 3) {
      throw new IllegalArgumentException("missing arguments");
    }
    SoakOptions options = new SoakOptions();
    options.engine = args[0].toLowerCase(Locale.ROOT);
    options.url = args[1];
    options.scenario = args[2].toLowerCase(Locale.ROOT);
    if (!List.of("live", "kill", "bootstrap", "all").contains(options.scenario)) {
      throw new IllegalArgumentException("unknown scenario " + args[2]);
    }
    for (int i = 3; i < args.length; i++) {
      String name = args[i];
      if ("--keep".equals(name)) {
        options.keep = true;
        continue;
      }
      if (i + 1 >= args.length) {
        throw new IllegalArgumentException("missing value for " + name);
      }
      String value = args[++i];
      switch (name) {
        case "--docs":
          options.docs = Integer.parseInt(value);
          break;
        case "--writers":
          options.writers = Integer.parseInt(value);
          break;
        case "--rate":
          options.rate = Double.parseDouble(value);
          break;
        case "--modes":
          options.modes = list(value);
          break;
        case "--phases":
          options.phases = new ArrayList<>();
          for (String phase : list(value)) {
            options.phases.add(phase.toUpperCase(Locale.ROOT));
          }
          break;
        case "--jvms":
          options.jvms = Integer.parseInt(value);
          break;
        case "--lock-ttl":
          options.lockTtl = Duration.ofMillis((long) (Double.parseDouble(value) * 1000));
          break;
        case "--swap-grace":
          options.swapGrace = Duration.ofMillis((long) (Double.parseDouble(value) * 1000));
          break;
        case "--post-seconds":
          options.post = Duration.ofMillis((long) (Double.parseDouble(value) * 1000));
          break;
        case "--out":
          options.out = Path.of(value);
          break;
        case "--label":
          options.label = value;
          break;
        case "--seed":
          options.seed = Long.parseLong(value);
          break;
        default:
          throw new IllegalArgumentException("unknown option " + name);
      }
    }
    return options;
  }

  /** Returns the seeded documents for a scenario. */
  int docs(String forScenario) {
    if (docs != null) {
      return docs;
    }
    switch (forScenario) {
      case "live":
        return 200_000;
      case "kill":
        return 50_000;
      default:
        return 10_000;
    }
  }

  private static List<String> list(String value) {
    List<String> items = new ArrayList<>();
    for (String item : value.split(",")) {
      if (!item.isBlank()) {
        items.add(item.trim());
      }
    }
    return items;
  }
}
