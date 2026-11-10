package com.phatjam98.elasticprotos.testkit.soak;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

/** Prints the human-readable summary of a soak run from its report section. */
final class Summary {

  private Summary() {}

  static void printLive(PrintStream out, Map<String, Object> report) {
    out.println("-- live " + report.get("mode") + ": migration " + report.get("migrationMs")
        + " ms, outcome " + Cluster.map(report.get("outcome")).get("status")
        + reconcile(report.get("outcome")));
    printLoad(out, report);
  }

  static void printKill(PrintStream out, Map<String, Object> report) {
    Map<String, Object> lock = Cluster.map(report.get("lock"));
    out.println("-- kill at " + report.get("killAt") + ": resume "
        + Cluster.map(report.get("resumePlan")).get("action") + " -> "
        + Cluster.map(report.get("resumeOutcome")).get("status")
        + reconcile(report.get("resumeOutcome")) + " in " + report.get("resumeMs")
        + " ms; lock taken over " + lock.get("takenOverAfterKillMs") + " ms after the kill"
        + " (bound " + lock.get("takeoverBoundMs") + " ms), left to expire: "
        + lock.get("staleLockLeftToExpire"));
    printLoad(out, report);
  }

  /** Returns the reconcile strategy and whether it swept, when the outcome reports them. */
  private static String reconcile(Object outcome) {
    Map<String, Object> reconcile = Cluster.map(Cluster.map(outcome).get("reconcile"));
    return reconcile.get("strategy") == null ? ""
        : " (" + reconcile.get("strategy") + ", swept " + reconcile.get("swept") + ")";
  }

  private static void printLoad(PrintStream out, Map<String, Object> report) {
    Map<String, Object> writes = Cluster.map(report.get("writes"));
    Map<String, Object> failures = Cluster.map(report.get("failures"));
    Map<String, Object> reads = Cluster.map(report.get("reads"));
    Map<String, Object> comparison = Cluster.map(report.get("comparison"));
    out.println("   documents: seeded " + report.get("seeded") + ", at end "
        + report.get("documentsAtEnd"));
    out.println("   writes: " + writes.get("attempts") + " attempts, " + writes.get("acknowledged")
        + " acknowledged; failed attempts " + failures.get("attemptsFailed") + " by class "
        + failures.get("byClass"));
    out.println("   failures by phase/op/kind: " + failures.get("byPhaseOpKind"));
    out.println("   reads: " + reads.get("reads") + ", stale " + reads.get("stale") + " by class "
        + reads.get("byClass") + "; documented stale window " + reads.get("firstStaleAfterSwapMs")
        + ".." + reads.get("lastStaleAfterSwapMs") + " ms after the swap (reconcile bound "
        + reads.get("swapToReconcileBoundMs") + " ms, swept " + reads.get("swapToSweptMs")
        + " ms)");
    out.println("   observations: " + Cluster.map(report.get("observations")).get("byClass"));
    out.println("   comparison: checked " + comparison.get("checked") + ", matched "
        + comparison.get("matched") + ", uncertain " + comparison.get("uncertainSkipped")
        + ", violations " + comparison.get("violations") + ", documented anomalies "
        + comparison.get("documentedAnomalies"));
    List<?> checks = (List<?>) report.get("checks");
    if (checks != null && !checks.isEmpty()) {
      out.println("   failed checks: " + checks);
    }
    out.println("   VIOLATIONS: " + report.get("violations"));
  }

  static void printBootstrap(PrintStream out, Map<String, Object> report) {
    out.println("-- bootstrap " + report.get("run") + ": start spread "
        + report.get("startSpreadMs") + " ms, exits " + report.get("exitCodes")
        + ", lock documents left " + report.get("lockDocumentsLeft"));
    Map<String, Object> aliases = Cluster.map(report.get("aliases"));
    aliases.forEach((alias, value) -> {
      Map<String, Object> summary = Cluster.map(value);
      out.println("   " + alias + ": planned " + summary.get("planned") + ", outcomes "
          + summary.get("outcomes") + ", indices " + summary.get("indices"));
    });
    List<?> checks = (List<?>) report.get("checks");
    if (!checks.isEmpty()) {
      out.println("   failed checks: " + checks);
    }
    out.println("   VIOLATIONS: " + report.get("violations"));
  }
}
