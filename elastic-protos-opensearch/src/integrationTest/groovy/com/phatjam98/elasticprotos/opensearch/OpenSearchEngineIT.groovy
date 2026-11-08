package com.phatjam98.elasticprotos.opensearch

import com.phatjam98.elasticprotos.engine.SearchEngine
import com.phatjam98.elasticprotos.testkit.EngineIntegrationSpec
import java.time.Duration
import org.opensearch.testcontainers.OpenSearchContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Shared

/**
 * Runs the shared engine specification against an OpenSearch container. The image comes from the
 * {@code elasticprotos.engineImage} system property, set by {@code -PengineImage=...}.
 */
class OpenSearchEngineIT extends EngineIntegrationSpec {

  static final String DEFAULT_IMAGE = 'opensearchproject/opensearch:3.9.0'

  @Shared OpenSearchContainer container

  @Override
  String engineImage() {
    String image = System.getProperty('elasticprotos.engineImage', '')
    image ? image : DEFAULT_IMAGE
  }

  @Override
  SearchEngine createEngine() {
    // The container runs with the security plugin disabled unless withSecurityEnabled() is called.
    container = new OpenSearchContainer(DockerImageName.parse(engineImage()))
        .withEnv('OPENSEARCH_JAVA_OPTS', '-Xms512m -Xmx512m')
        .withStartupTimeout(Duration.ofMinutes(3))
    container.start()
    OpenSearchEngine.builder().hosts(container.httpHostAddress).build()
  }

  @Override
  void releaseEngine() {
    container?.stop()
  }
}
