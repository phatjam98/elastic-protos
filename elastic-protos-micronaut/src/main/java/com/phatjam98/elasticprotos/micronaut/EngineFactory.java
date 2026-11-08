package com.phatjam98.elasticprotos.micronaut;

import com.phatjam98.elasticprotos.elasticsearch.ElasticsearchEngine;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.opensearch.OpenSearchEngine;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Secondary;
import io.micronaut.core.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import javax.net.ssl.SSLContext;

/**
 * Creates the {@link SearchEngine} bean from {@link ElasticProtosConfiguration}: the Elasticsearch
 * adapter when {@code elastic-protos.engine} is {@code elasticsearch} (the default) and the
 * OpenSearch adapter when it is {@code opensearch}, each only when that adapter is on the
 * classpath.
 *
 * <p>Both beans are {@link Secondary}: an application that defines its own {@code SearchEngine}
 * bean gets that one injected everywhere instead, without {@code @Replaces}. Building an engine
 * does not connect to the cluster. A {@link SSLContext} bean named {@code elastic-protos}, when
 * present, is used for TLS.
 */
@Factory
public class EngineFactory {

  /** Name of the optional {@link SSLContext} bean used by the engines. */
  public static final String SSL_CONTEXT_NAME = "elastic-protos";

  /**
   * Creates the Elasticsearch engine.
   *
   * @param config the configuration
   * @param sslContext an optional TLS context bean named {@code elastic-protos}
   * @return the engine; closed when the context stops
   */
  @Singleton
  @Secondary
  @Bean(preDestroy = "close")
  @Requires(property = "elastic-protos.engine", value = ElasticProtosConfiguration.ELASTICSEARCH,
      defaultValue = ElasticProtosConfiguration.ELASTICSEARCH)
  @Requires(classes = ElasticsearchEngine.class)
  public SearchEngine elasticsearchEngine(ElasticProtosConfiguration config,
      @Nullable @Named(SSL_CONTEXT_NAME) SSLContext sslContext) {
    return ElasticsearchSupport.create(config, sslContext);
  }

  /**
   * Creates the OpenSearch engine.
   *
   * @param config the configuration
   * @param sslContext an optional TLS context bean named {@code elastic-protos}
   * @return the engine; closed when the context stops
   */
  @Singleton
  @Secondary
  @Bean(preDestroy = "close")
  @Requires(property = "elastic-protos.engine", value = ElasticProtosConfiguration.OPENSEARCH)
  @Requires(classes = OpenSearchEngine.class)
  public SearchEngine openSearchEngine(ElasticProtosConfiguration config,
      @Nullable @Named(SSL_CONTEXT_NAME) SSLContext sslContext) {
    return OpenSearchSupport.create(config, sslContext);
  }
}
