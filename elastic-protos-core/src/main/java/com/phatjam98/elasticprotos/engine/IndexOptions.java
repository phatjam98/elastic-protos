package com.phatjam98.elasticprotos.engine;

/**
 * Options for a single-document write.
 *
 * <p>{@code ifSeqNo} and {@code ifPrimaryTerm} make the write conditional (optimistic
 * concurrency): it only succeeds while the document still has that sequence number and primary
 * term, as read from {@link IndexResult} or {@link Hit}, and otherwise fails with {@link
 * EngineException.Kind#CONFLICT}. Set both or neither.
 *
 * @param createOnly fail if a document with the id already exists
 * @param refresh when the write becomes visible to search
 * @param ifSeqNo the sequence number the document must have, or null
 * @param ifPrimaryTerm the primary term the document must have, or null
 */
public record IndexOptions(boolean createOnly, Refresh refresh, Long ifSeqNo,
    Long ifPrimaryTerm) {

  /** Refresh policies. */
  public enum Refresh {
    NONE,
    IMMEDIATE,
    WAIT_FOR
  }

  /**
   * Checks that the sequence number and primary term are both set or both null.
   *
   * @throws IllegalArgumentException when only one of them is set
   */
  public IndexOptions {
    if ((ifSeqNo == null) != (ifPrimaryTerm == null)) {
      throw new IllegalArgumentException(
          "ifSeqNo and ifPrimaryTerm must be set together: " + ifSeqNo + ", " + ifPrimaryTerm);
    }
  }

  /**
   * Unconditional write options.
   *
   * @param createOnly fail if a document with the id already exists
   * @param refresh when the write becomes visible to search
   */
  public IndexOptions(boolean createOnly, Refresh refresh) {
    this(createOnly, refresh, null, null);
  }

  /** Overwrite, no refresh. */
  public static IndexOptions defaults() {
    return new IndexOptions(false, Refresh.NONE);
  }

  /** Overwrite and refresh immediately, for tests and small writes. */
  public static IndexOptions refreshed() {
    return new IndexOptions(false, Refresh.IMMEDIATE);
  }

  /**
   * Overwrite without refresh, but only while the document has this sequence number and primary
   * term.
   *
   * @param seqNo the expected sequence number
   * @param primaryTerm the expected primary term
   * @return the options
   */
  public static IndexOptions ifMatch(long seqNo, long primaryTerm) {
    return new IndexOptions(false, Refresh.NONE, seqNo, primaryTerm);
  }

  /** Returns whether the write is conditional on a sequence number and primary term. */
  public boolean conditional() {
    return ifSeqNo != null;
  }

  /** Returns a copy with another refresh policy. */
  public IndexOptions withRefresh(Refresh value) {
    return new IndexOptions(createOnly, value, ifSeqNo, ifPrimaryTerm);
  }

  /** Returns a copy that does or does not fail when the document exists. */
  public IndexOptions withCreateOnly(boolean value) {
    return new IndexOptions(value, refresh, ifSeqNo, ifPrimaryTerm);
  }
}
