package com.phatjam98.elasticprotos.elasticsearch

import com.phatjam98.elasticprotos.engine.SearchEngine
import com.phatjam98.elasticprotos.testkit.EngineIntegrationSpec
import java.time.Duration
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Shared

/**
 * Runs the shared engine specification against an Elasticsearch container. The image comes from
 * the {@code elasticprotos.engineImage} system property, set by {@code -PengineImage=...}.
 */
class ElasticsearchEngineIT extends EngineIntegrationSpec {

  static final String DEFAULT_IMAGE = 'docker.elastic.co/elasticsearch/elasticsearch:9.5.5'

  @Shared ElasticsearchContainer container

  @Override
  String engineImage() {
    String image = System.getProperty('elasticprotos.engineImage', '')
    image ? image : DEFAULT_IMAGE
  }

  @Override
  SearchEngine createEngine() {
    container = new ElasticsearchContainer(DockerImageName.parse(engineImage())
        .asCompatibleSubstituteFor('docker.elastic.co/elasticsearch/elasticsearch'))
        .withEnv('xpack.security.enabled', 'false')
        .withEnv('ES_JAVA_OPTS', '-Xms512m -Xmx512m')
        .withStartupTimeout(Duration.ofMinutes(3))
    container.start()
    ElasticsearchEngine.builder().hosts('http://' + container.httpHostAddress).build()
  }

  @Override
  void releaseEngine() {
    container?.stop()
  }
}
