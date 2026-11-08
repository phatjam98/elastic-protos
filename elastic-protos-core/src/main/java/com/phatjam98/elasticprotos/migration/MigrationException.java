package com.phatjam98.elasticprotos.migration;

/** A migration step failed; any partial work has been rolled back where possible. */
public class MigrationException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates an exception.
   *
   * @param message what failed
   */
  public MigrationException(String message) {
    super(message);
  }

  /**
   * Creates an exception with a cause.
   *
   * @param message what failed
   * @param cause the underlying failure
   */
  public MigrationException(String message, Throwable cause) {
    super(message, cause);
  }
}
