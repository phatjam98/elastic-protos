package com.acme.search;

import io.micronaut.runtime.Micronaut;

/** Starts the sample. elastic-protos bootstraps the indices on startup. */
public final class Application {

  private Application() {}

  /**
   * Runs the application.
   *
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    Micronaut.run(Application.class, args);
  }
}
