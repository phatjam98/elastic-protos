package com.phatjam98.elasticprotos.query;

/** Thrown when search criteria cannot be compiled against an index definition. */
public class QueryCompileException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final String field;

  /**
   * Creates an exception.
   *
   * @param message what is wrong and how to fix it
   * @param field the field the problem is about, or null when it is not about one field
   */
  public QueryCompileException(String message, String field) {
    super(message);
    this.field = field;
  }

  /** Returns the field the problem is about, or null. */
  public String getField() {
    return field;
  }
}
