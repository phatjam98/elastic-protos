package com.phatjam98.elasticprotos.opensearch;

import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.engine.rest.JsonRestEngine;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;

/**
 * {@link SearchEngine} for OpenSearch 2.x and 3.x, built on the official low-level REST client.
 *
 * <pre>{@code
 * SearchEngine engine = OpenSearchEngine.builder()
 *     .hosts("https://opensearch.example.com:9200")
 *     .basicAuth("admin", "secret")
 *     .build();
 * }</pre>
 */
public final class OpenSearchEngine {

  private OpenSearchEngine() {}

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

  /** Builder for an OpenSearch engine. */
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
        try {
          hosts.add(HttpHost.create(url));
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException("Not a host URL: " + url, e);
        }
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
            new Header[] {new BasicHeader("Authorization", "ApiKey " + apiKey)});
      }
      SSLContext context = sslContext;
      if (insecureTrustAllCertificates) {
        try {
          context = SSLContextBuilder.create().loadTrustMaterial(null, TrustAllStrategy.INSTANCE)
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
          credentials.setCredentials(new AuthScope(null, -1),
              new UsernamePasswordCredentials(username, password.toCharArray()));
          httpClient.setDefaultCredentialsProvider(credentials);
        }
        if (effectiveContext != null || noHostnameCheck) {
          ClientTlsStrategyBuilder tls = ClientTlsStrategyBuilder.create();
          if (effectiveContext != null) {
            tls.setSslContext(effectiveContext);
          }
          if (noHostnameCheck) {
            tls.setHostnameVerifier(NoopHostnameVerifier.INSTANCE);
          }
          httpClient.setConnectionManager(
              PoolingAsyncClientConnectionManagerBuilder.create().setTlsStrategy(tls.build())
                  .build());
        }
        return httpClient;
      });
      return of(builder.build());
    }
  }
}
