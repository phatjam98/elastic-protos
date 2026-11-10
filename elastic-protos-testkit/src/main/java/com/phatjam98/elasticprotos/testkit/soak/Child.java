package com.phatjam98.elasticprotos.testkit.soak;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** A child JVM whose output goes to a log file; its {@code SOAK } lines go to a callback. */
final class Child {

  private final Process process;
  private final Thread pump;
  private final ArrayDeque<String> tail = new ArrayDeque<>();

  private Child(Process process, Path log, Consumer<String> onLine) throws IOException {
    this.process = process;
    BufferedWriter writer = Files.newBufferedWriter(log, StandardCharsets.UTF_8);
    this.pump = new Thread(() -> {
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
          BufferedWriter out = writer) {
        String line;
        while ((line = reader.readLine()) != null) {
          out.write(line);
          out.newLine();
          out.flush();
          synchronized (tail) {
            tail.addLast(line);
            if (tail.size() > 20) {
              tail.removeFirst();
            }
          }
          if (line.startsWith("SOAK ")) {
            onLine.accept(line.substring(5));
          }
        }
      } catch (IOException e) {
        // the process died; its output ends here
      }
    }, "soak-child-output");
    pump.setDaemon(true);
    pump.start();
  }

  /**
   * Starts a child.
   *
   * @param command the command line
   * @param log where its output goes
   * @param onLine called with every {@code SOAK } line, without the prefix
   * @return the child
   */
  static Child start(List<String> command, Path log, Consumer<String> onLine) throws IOException {
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    return new Child(process, log, onLine);
  }

  /** Kills the process hard ({@code SIGKILL} on Unix) and waits for it. */
  void kill() throws InterruptedException {
    process.destroyForcibly();
    process.waitFor();
    pump.join(5000);
  }

  /** Waits for the process to exit; returns its exit code, or null on timeout. */
  Integer await(long seconds) throws InterruptedException {
    if (!process.waitFor(seconds, TimeUnit.SECONDS)) {
      return null;
    }
    pump.join(5000);
    return process.exitValue();
  }

  boolean alive() {
    return process.isAlive();
  }

  long pid() {
    return process.pid();
  }

  /** Returns the last lines of output. */
  List<String> tail() {
    synchronized (tail) {
      return List.copyOf(tail);
    }
  }
}
