package com.phatjam98.elasticsearch.micronaut.factory;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.annotation.Nullable;
import jakarta.inject.Singleton;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the Elasticsearch Client Factory.
 */
@Factory
public class ClientFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientFactory.class);

  @Property(name = "elasticsearch.username")
  @Nullable
  private String username;

  @Property(name = "elasticsearch.password")
  @Nullable
  private String password;

  @Value("${elasticsearch.insecure-trust-all-certificates:false}")
  private boolean insecureTrustAllCertificates;

  @Replaces(HttpAsyncClientBuilder.class)
  @Singleton
  HttpAsyncClientBuilder builder() {
    HttpAsyncClientBuilder builder = HttpAsyncClientBuilder.create();
    CredentialsProvider credentialsProvider = null;

    if (username != null && password != null) {
      credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY,
              new UsernamePasswordCredentials(username, password));

      builder.setDefaultCredentialsProvider(credentialsProvider);
    }

    final HostnameVerifier hostnameVerifier =
            insecureTrustAllCertificates ? NoopHostnameVerifier.INSTANCE : null;
    builder.setSSLHostnameVerifier(hostnameVerifier);

    SSLContext sslContext = null;
    try {
      if (insecureTrustAllCertificates) {
        sslContext = new SSLContextBuilder()
                .loadTrustMaterial(null, TrustAllStrategy.INSTANCE).build();
      }
    } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
      throw new RuntimeException(e);
    }

    builder.setSSLContext(sslContext);

    return builder;
  }
}
