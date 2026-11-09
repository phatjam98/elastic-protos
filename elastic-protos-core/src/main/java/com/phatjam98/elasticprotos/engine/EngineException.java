package com.phatjam98.elasticprotos.engine;

/** An error reported by the engine or the transport, classified for callers. */
public class EngineException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** Error classes callers may want to branch on. */
  public enum Kind {
    NOT_FOUND,
    ALREADY_EXISTS,
    CONFLICT,
    BAD_REQUEST,
    UNAUTHORIZED,
    /**
     * The index or cluster is blocked: a {@code cluster_block_exception} with a 4xx status (for
     * example an {@code index.blocks.write}, read or read-only block, or the flood-stage disk
     * watermark's {@code read_only_allow_delete} block) or an {@code index_closed_exception}.
     * Retrying does not help until the block is lifted, so it is not retried. A 5xx {@code
     * cluster_block_exception} (state not recovered, no master) is {@link #TRANSIENT} instead.
     */
    BLOCKED,
    TRANSIENT,
    TIMEOUT,
    IO,
    OTHER
  }

  private final Kind kind;
  private final int status;

  /**
   * Creates an exception.
   *
   * @param kind the class of error
   * @param status the HTTP status, or 0 when none
   * @param message the reason
   */
  public EngineException(Kind kind, int status, String message) {
    this(kind, status, message, null);
  }

  /**
   * Creates an exception with a cause.
   *
   * @param kind the class of error
   * @param status the HTTP status, or 0 when none
   * @param message the reason
   * @param cause the underlying failure
   */
  public EngineException(Kind kind, int status, String message, Throwable cause) {
    super(message, cause);
    this.kind = kind;
    this.status = status;
  }

  /** Returns the class of error. */
  public Kind getKind() {
    return kind;
  }

  /** Returns the HTTP status, or 0 when none. */
  public int getStatus() {
    return status;
  }
}
