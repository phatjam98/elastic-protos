package com.phatjam98.elasticprotos.opensearch;

import com.phatjam98.elasticprotos.engine.rest.RawHttp;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;

/** {@link RawHttp} over the OpenSearch low-level REST client. */
final class RestClientHttp implements RawHttp {

  private final RestClient client;

  RestClientHttp(RestClient client) {
    this.client = client;
  }

  @Override
  public Response perform(
      String method, String path, Map<String, String> params, String body, String contentType)
      throws IOException {
    Request request = new Request(method, path);
    request.addParameters(params);
    if (body != null) {
      request.setEntity(
          new StringEntity(body, ContentType.create(contentType, StandardCharsets.UTF_8)));
    }
    try {
      return toResponse(client.performRequest(request));
    } catch (ResponseException e) {
      return toResponse(e.getResponse());
    }
  }

  private static Response toResponse(org.opensearch.client.Response response)
      throws IOException {
    try {
      String body = response.getEntity() == null
          ? ""
          : EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
      return new Response(response.getStatusLine().getStatusCode(), body);
    } catch (ParseException e) {
      throw new IOException("Unreadable response body", e);
    }
  }

  /** Lists the client's nodes, for example {@code http://localhost:9200}. */
  @Override
  public String describe() {
    return client.getNodes().stream()
        .map(node -> node.getHost().toURI())
        .collect(Collectors.joining(", "));
  }

  @Override
  public void close() throws IOException {
    client.close();
  }
}
