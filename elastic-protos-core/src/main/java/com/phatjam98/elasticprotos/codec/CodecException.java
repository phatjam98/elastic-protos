package com.phatjam98.elasticprotos.codec;

/** Thrown when a message cannot be converted to a document or back. */
public class CodecException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** Creates an exception with a message. */
  public CodecException(String message) {
    super(message);
  }

  /** Creates an exception with a message and a cause. */
  public CodecException(String message, Throwable cause) {
    super(message, cause);
  }
}
