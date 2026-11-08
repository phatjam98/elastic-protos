package com.phatjam98.elasticprotos.engine.rest;

import java.io.IOException;
import java.util.Map;

/**
 * The minimal HTTP transport an adapter must provide: send a request, get a status and a body.
 * Non-2xx responses are returned, not thrown, so that {@link JsonRestEngine} can classify them.
 */
public interface RawHttp extends AutoCloseable {

  /** JSON content type. */
  String JSON = "application/json";

  /** Newline-delimited JSON content type used by the bulk API. */
  String NDJSON = "application/x-ndjson";

  /**
   * An HTTP response.
   *
   * @param status the HTTP status
   * @param body the body, possibly empty
   */
  record Response(int status, String body) {

    /** Returns whether the status is 2xx. */
    public boolean ok() {
      return status >= 200 && status < 300;
    }
  }

  /**
   * Performs a request.
   *
   * @param method {@code GET}, {@code HEAD}, {@code PUT}, {@code POST} or {@code DELETE}
   * @param path the path including the leading slash
   * @param params query parameters, possibly empty
   * @param body the request body, or null
   * @param contentType the body's content type, ignored when there is no body
   * @return the response, whatever its status
   * @throws IOException if the request could not be completed
   */
  Response perform(
      String method, String path, Map<String, String> params, String body, String contentType)
      throws IOException;

  /**
   * Describes where requests go, for error messages; for example {@code http://localhost:9200}.
   * Adapters list the nodes of their client. The default implementation returns an empty string,
   * which leaves the endpoint out of messages.
   *
   * @return the endpoints, comma separated, or an empty string when unknown
   */
  default String describe() {
    return "";
  }

  @Override
  void close() throws IOException;
}
