package com.phatjam98.elasticprotos.micronaut;

import com.phatjam98.elasticprotos.elasticsearch.ElasticsearchEngine;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import javax.net.ssl.SSLContext;

/**
 * Builds the Elasticsearch adapter. Kept apart from {@link EngineFactory} so that the adapter's
 * classes are only loaded when its bean is actually created.
 */
final class ElasticsearchSupport {

  private ElasticsearchSupport() {}

  static SearchEngine create(ElasticProtosConfiguration config, SSLContext sslContext) {
    ElasticsearchEngine.Builder builder = ElasticsearchEngine.builder()
        .hosts(config.getHosts().toArray(new String[0]))
        .insecureTrustAllCertificates(config.getSsl().isInsecureTrustAllCertificates());
    if (config.getUsername() != null) {
      builder.basicAuth(config.getUsername(), config.getPassword());
    }
    if (config.getApiKey() != null) {
      builder.apiKey(config.getApiKey());
    }
    if (sslContext != null) {
      builder.sslContext(sslContext);
    }
    return builder.build();
  }
}
