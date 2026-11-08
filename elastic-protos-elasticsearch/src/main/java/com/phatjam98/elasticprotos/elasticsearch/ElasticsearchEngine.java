package com.phatjam98.elasticprotos.elasticsearch;

import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.engine.rest.JsonRestEngine;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

/**
 * {@link SearchEngine} for Elasticsearch 8.x and 9.x, built on the official low-level REST
 * client.
 *
 * <pre>{@code
 * SearchEngine engine = ElasticsearchEngine.builder()
 *     .hosts("https://es.example.com:9200")
 *     .basicAuth("elastic", "secret")
 *     .build();
 * }</pre>
 */
public final class ElasticsearchEngine {

  private ElasticsearchEngine() {}

  /**
   * Wraps an existing client. Closing the engine closes the client.
   *
   * @param client a configured low-level REST client
   * @return the engine
   */
  public static SearchEngine of(RestClient client) {
    return new JsonRestEngine(new RestClientHttp(client));
  }

  /** Starts building an engine from connection settings. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for an Elasticsearch engine. */
  public static final class Builder {

    private final List<HttpHost> hosts = new ArrayList<>();
    private String username;
    private String password;
    private String apiKey;
    private SSLContext sslContext;
    private boolean insecureTrustAllCertificates;

    private Builder() {}

    /** Adds hosts as URLs, for example {@code http://localhost:9200}. */
    public Builder hosts(String... urls) {
      for (String url : urls) {
        hosts.add(HttpHost.create(url));
      }
      return this;
    }

    /** Uses basic authentication. */
    public Builder basicAuth(String user, String pass) {
      this.username = user;
      this.password = pass;
      return this;
    }

    /** Uses an API key, given as the base64 {@code id:api_key} value or as {@code id:api_key}. */
    public Builder apiKey(String key) {
      this.apiKey = key.contains(":")
          ? Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8))
          : key;
      return this;
    }

    /** Uses a custom SSL context, for example one that trusts a private CA. */
    public Builder sslContext(SSLContext context) {
      this.sslContext = context;
      return this;
    }

    /**
     * Disables certificate and hostname verification. Only for local development; traffic is
     * still encrypted but open to interception.
     */
    public Builder insecureTrustAllCertificates(boolean insecure) {
      this.insecureTrustAllCertificates = insecure;
      return this;
    }

    /** Builds the engine. */
    public SearchEngine build() {
      if (hosts.isEmpty()) {
        throw new IllegalStateException("at least one host is required");
      }
      RestClientBuilder builder = RestClient.builder(hosts.toArray(new HttpHost[0]));
      if (apiKey != null) {
        builder.setDefaultHeaders(
            new BasicHeader[] {new BasicHeader("Authorization", "ApiKey " + apiKey)});
      }
      SSLContext context = sslContext;
      if (insecureTrustAllCertificates) {
        try {
          context = new SSLContextBuilder().loadTrustMaterial(null, TrustAllStrategy.INSTANCE)
              .build();
        } catch (GeneralSecurityException e) {
          throw new IllegalStateException("Cannot build a trust-all SSL context", e);
        }
      }
      SSLContext effectiveContext = context;
      boolean noHostnameCheck = insecureTrustAllCertificates;
      builder.setHttpClientConfigCallback(httpClient -> {
        if (username != null) {
          BasicCredentialsProvider credentials = new BasicCredentialsProvider();
          credentials.setCredentials(AuthScope.ANY,
              new UsernamePasswordCredentials(username, password));
          httpClient.setDefaultCredentialsProvider(credentials);
        }
        if (effectiveContext != null) {
          httpClient.setSSLContext(effectiveContext);
        }
        if (noHostnameCheck) {
          httpClient.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
        }
        return httpClient;
      });
      return of(builder.build());
    }
  }
}
