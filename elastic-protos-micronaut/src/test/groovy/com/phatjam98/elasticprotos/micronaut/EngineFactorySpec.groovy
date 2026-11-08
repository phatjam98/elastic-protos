package com.phatjam98.elasticprotos.micronaut

import com.phatjam98.elasticprotos.engine.SearchEngine
import com.phatjam98.elasticprotos.engine.rest.JsonRestEngine
import com.phatjam98.elasticprotos.testkit.InMemorySearchEngine
import io.micronaut.context.ApplicationContext
import spock.lang.Specification

class EngineFactorySpec extends Specification {

  static final Map ES_PROPS = [
      'elastic-protos.engine'  : 'elasticsearch',
      'elastic-protos.username': 'u',
      'elastic-protos.password': 'p',
      'elastic-protos.api-key' : 'id:key',
      'elastic-protos.hosts'   : ['http://es1:9200', 'http://es2:9200'],
  ]

  static final Map OS_PROPS = [
      'elastic-protos.engine'                             : 'opensearch',
      'elastic-protos.username'                           : 'u',
      'elastic-protos.password'                           : 'p',
      'elastic-protos.api-key'                            : 'abc',
      'elastic-protos.ssl.insecure-trust-all-certificates': true,
  ]

  def "builds the #engine adapter from the configuration without connecting"() {
    given:
    def ctx = ApplicationContext.run(props)

    when:
    def bean = ctx.getBean(SearchEngine)

    then:
    bean instanceof JsonRestEngine
    bean.@http.class.name == transport
    ctx.getBeansOfType(SearchEngine).size() == 1
    ctx.getBean(ElasticProtosService).engine().is(bean)

    cleanup:
    ctx?.close()

    where:
    engine          | transport                                                    | props
    'default'       | 'com.phatjam98.elasticprotos.elasticsearch.RestClientHttp' | [:]
    'elasticsearch' | 'com.phatjam98.elasticprotos.elasticsearch.RestClientHttp' | ES_PROPS
    'opensearch'    | 'com.phatjam98.elasticprotos.opensearch.RestClientHttp'    | OS_PROPS
  }

  def "an unknown engine name leaves no engine and no elastic-protos beans"() {
    given:
    def ctx = ApplicationContext.run(['elastic-protos.engine': 'solr'])

    expect:
    !ctx.containsBean(SearchEngine)
    !ctx.containsBean(ElasticProtosService)
    !ctx.containsBean(ElasticProtosBootstrap)

    cleanup:
    ctx.close()
  }

  def "a user-defined engine bean wins over the factory's secondary bean"() {
    given:
    def ctx = ApplicationContext.run(['test.engine': 'memory'])

    expect:
    ctx.getBean(SearchEngine) instanceof InMemorySearchEngine
    ctx.getBean(ElasticProtosService).engine() instanceof InMemorySearchEngine

    cleanup:
    ctx.close()
  }

  def "a named SSLContext bean is passed to the engine"() {
    given:
    def ctx = ApplicationContext.builder()
        .properties(['elastic-protos.engine': engine])
        .start()
    ctx.registerSingleton(javax.net.ssl.SSLContext, javax.net.ssl.SSLContext.getDefault(),
        io.micronaut.inject.qualifiers.Qualifiers.byName(EngineFactory.SSL_CONTEXT_NAME))

    expect:
    ctx.getBean(SearchEngine) instanceof JsonRestEngine

    cleanup:
    ctx.close()

    where:
    engine << ['elasticsearch', 'opensearch']
  }
}
