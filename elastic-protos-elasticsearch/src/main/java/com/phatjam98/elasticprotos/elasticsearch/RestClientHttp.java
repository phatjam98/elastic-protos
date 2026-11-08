package com.phatjam98.elasticprotos.elasticsearch;

import com.phatjam98.elasticprotos.engine.rest.RawHttp;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

/** {@link RawHttp} over the Elasticsearch low-level REST client. */
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
      request.setEntity(new NStringEntity(body, ContentType.create(contentType, "UTF-8")));
    }
    try {
      return toResponse(client.performRequest(request));
    } catch (ResponseException e) {
      return toResponse(e.getResponse());
    }
  }

  private static Response toResponse(org.elasticsearch.client.Response response)
      throws IOException {
    String body = response.getEntity() == null ? "" : EntityUtils.toString(response.getEntity());
    return new Response(response.getStatusLine().getStatusCode(), body);
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
