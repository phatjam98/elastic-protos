package com.phatjam98.elasticprotos.engine;

/**
 * Options for a single-document write.
 *
 * @param createOnly fail if a document with the id already exists
 * @param refresh when the write becomes visible to search
 */
public record IndexOptions(boolean createOnly, Refresh refresh) {

  /** Refresh policies. */
  public enum Refresh {
    NONE,
    IMMEDIATE,
    WAIT_FOR
  }

  /** Overwrite, no refresh. */
  public static IndexOptions defaults() {
    return new IndexOptions(false, Refresh.NONE);
  }

  /** Overwrite and refresh immediately, for tests and small writes. */
  public static IndexOptions refreshed() {
    return new IndexOptions(false, Refresh.IMMEDIATE);
  }
}
