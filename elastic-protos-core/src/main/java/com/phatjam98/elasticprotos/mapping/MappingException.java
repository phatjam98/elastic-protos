package com.phatjam98.elasticprotos.mapping;

/** Thrown when a message cannot be turned into a valid index mapping. */
public class MappingException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final String path;

  /**
   * Creates an exception about a field path.
   *
   * @param path the dotted field path, or the message name for message-level problems
   * @param message what is wrong
   */
  public MappingException(String path, String message) {
    super(path + ": " + message);
    this.path = path;
  }

  /** Returns the field path or message name the problem was found at. */
  public String getPath() {
    return path;
  }
}
