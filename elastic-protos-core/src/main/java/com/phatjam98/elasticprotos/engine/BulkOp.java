package com.phatjam98.elasticprotos.engine;

import java.util.Map;

/**
 * One operation in a bulk request.
 *
 * @param type what to do
 * @param index the target index or alias
 * @param id the document id, or null to let the engine generate one
 * @param source the document, the partial document for {@code UPDATE}, or null for {@code DELETE}
 */
public record BulkOp(Type type, String index, String id, Map<String, Object> source) {

  /** The kinds of bulk operation. */
  public enum Type {
    INDEX,
    CREATE,
    UPDATE,
    DELETE
  }

  /** Indexes a document, overwriting any existing one. */
  public static BulkOp index(String index, String id, Map<String, Object> source) {
    return new BulkOp(Type.INDEX, index, id, source);
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
}
