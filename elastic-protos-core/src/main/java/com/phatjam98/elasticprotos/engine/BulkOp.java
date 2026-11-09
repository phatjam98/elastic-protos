package com.phatjam98.elasticprotos.engine;

import java.util.Map;

/**
 * One operation in a bulk request.
 *
 * <p>{@code ifSeqNo} and {@code ifPrimaryTerm} make an operation conditional: the item fails with
 * status 409 unless the document still has that sequence number and primary term. Set both or
 * neither.
 *
 * @param type what to do
 * @param index the target index or alias
 * @param id the document id, or null to let the engine generate one
 * @param source the document, the partial document for {@code UPDATE}, or null for {@code DELETE}
 * @param ifSeqNo the sequence number the document must have, or null
 * @param ifPrimaryTerm the primary term the document must have, or null
 */
public record BulkOp(Type type, String index, String id, Map<String, Object> source,
    Long ifSeqNo, Long ifPrimaryTerm) {

  /** The kinds of bulk operation. */
  public enum Type {
    INDEX,
    CREATE,
    UPDATE,
    DELETE
  }

  /**
   * Checks the conditional settings the engines would reject; in a bulk request one such item
   * fails the whole request.
   *
   * @throws IllegalArgumentException when only one of the sequence number and primary term is
   *     set, when the sequence number is negative or the primary term below 1, or when a {@code
   *     CREATE} is conditional
   */
  public BulkOp {
    IndexOptions.checkIfMatch(ifSeqNo, ifPrimaryTerm);
    if (type == Type.CREATE && ifSeqNo != null) {
      throw new IllegalArgumentException(
          "create operations do not support compare and set: use INDEX with ifSeqNo");
    }
  }

  /**
   * An unconditional operation.
   *
   * @param type what to do
   * @param index the target index or alias
   * @param id the document id, or null to let the engine generate one
   * @param source the document, the partial document, or null for {@code DELETE}
   */
  public BulkOp(Type type, String index, String id, Map<String, Object> source) {
    this(type, index, id, source, null, null);
  }

  /** Indexes a document, overwriting any existing one. */
  public static BulkOp index(String index, String id, Map<String, Object> source) {
    return new BulkOp(Type.INDEX, index, id, source);
  }

  /**
   * Overwrites a document only while it has this sequence number and primary term.
   *
   * @param index the target index or alias
   * @param id the document id
   * @param source the document
   * @param seqNo the expected sequence number
   * @param primaryTerm the expected primary term
   * @return the operation
   */
  public static BulkOp indexIfMatch(String index, String id, Map<String, Object> source,
      long seqNo, long primaryTerm) {
    return new BulkOp(Type.INDEX, index, id, source, seqNo, primaryTerm);
  }

  /** Creates a document, failing if the id exists. */
  public static BulkOp create(String index, String id, Map<String, Object> source) {
    return new BulkOp(Type.CREATE, index, id, source);
  }

  /** Merges a partial document into an existing one. */
  public static BulkOp update(String index, String id, Map<String, Object> partial) {
    return new BulkOp(Type.UPDATE, index, id, partial);
  }

  /** Deletes a document. */
  public static BulkOp delete(String index, String id) {
    return new BulkOp(Type.DELETE, index, id, null);
  }

  /**
   * Deletes a document only while it has this sequence number and primary term.
   *
   * @param index the target index or alias
   * @param id the document id
   * @param seqNo the expected sequence number
   * @param primaryTerm the expected primary term
   * @return the operation
   */
  public static BulkOp deleteIfMatch(String index, String id, long seqNo, long primaryTerm) {
    return new BulkOp(Type.DELETE, index, id, null, seqNo, primaryTerm);
  }
}
