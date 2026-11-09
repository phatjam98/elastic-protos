package com.phatjam98.elasticprotos.migration;

/**
 * The per-alias migration lock could not be taken, or was lost while it was held.
 *
 * <p>A lock another process held for longer than {@link MigrationOptions#lockWait()} makes the
 * item {@link MigrationResult.Status#LOCKED}; a lock lost while migrating (its document was taken
 * over or deleted, typically after the heartbeat could not reach the cluster for longer than
 * {@link MigrationOptions#lockTtl()}) makes it {@link MigrationResult.Status#FAILED}, and the next
 * run resumes the interrupted work.
 */
public class LockException extends MigrationException {

  private static final long serialVersionUID = 1L;

  private final String holder;
  private final boolean lost;

  /**
   * Creates an exception.
   *
   * @param message the reason
   * @param holder the holder of the lock, when known
   * @param lost whether a lock this process held was lost, as opposed to never taken
   */
  public LockException(String message, String holder, boolean lost) {
    super(message);
    this.holder = holder;
    this.lost = lost;
  }

  /** Returns the holder of the lock, as recorded in the lock document, or null when unknown. */
  public String holder() {
    return holder;
  }

  /** Returns whether a lock this process held was lost, as opposed to never taken. */
  public boolean lost() {
    return lost;
  }
}
