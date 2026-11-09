package com.phatjam98.elasticprotos.engine;

/**
 * Options for a single-document write.
 *
 * <p>{@code ifSeqNo} and {@code ifPrimaryTerm} make the write conditional (optimistic
 * concurrency): it only succeeds while the document still has that sequence number and primary
 * term, as read from {@link IndexResult} or {@link Hit}, and otherwise fails with {@link
 * EngineException.Kind#CONFLICT}. Set both or neither; a conditional write needs a document id and
 * cannot be create-only.
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
   * Checks the conditional-write settings the engines would reject.
   *
   * @throws IllegalArgumentException when only one of the sequence number and primary term is
   *     set, when the sequence number is negative or the primary term below 1, or when a
   *     conditional write is also create-only, which the engines do not support
   */
  public IndexOptions {
    checkIfMatch(ifSeqNo, ifPrimaryTerm);
    if (createOnly && ifSeqNo != null) {
      throw new IllegalArgumentException(
          "create operations do not support compare and set: use ifSeqNo/ifPrimaryTerm or"
              + " createOnly, not both");
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

  /** Validates a sequence number and primary term pair; shared with {@link BulkOp}. */
  static void checkIfMatch(Long ifSeqNo, Long ifPrimaryTerm) {
    if ((ifSeqNo == null) != (ifPrimaryTerm == null)) {
      throw new IllegalArgumentException(
          "ifSeqNo and ifPrimaryTerm must be set together: " + ifSeqNo + ", " + ifPrimaryTerm);
    }
    if (ifSeqNo != null && ifSeqNo < 0) {
      throw new IllegalArgumentException("ifSeqNo must not be negative: " + ifSeqNo);
    }
    if (ifPrimaryTerm != null && ifPrimaryTerm < 1) {
      throw new IllegalArgumentException("ifPrimaryTerm must be at least 1: " + ifPrimaryTerm);
    }
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
