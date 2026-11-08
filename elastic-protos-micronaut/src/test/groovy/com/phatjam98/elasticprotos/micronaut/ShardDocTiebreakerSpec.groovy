package com.phatjam98.elasticprotos.micronaut

import com.phatjam98.elasticprotos.engine.EngineInfo
import com.phatjam98.elasticprotos.engine.EngineProduct
import com.phatjam98.elasticprotos.engine.SearchResult
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.query.Criteria
import com.phatjam98.elasticprotos.query.QueryCompilerConfig
import com.phatjam98.elasticprotos.testkit.InMemorySearchEngine
import com.phatjam98.elasticprotos.testkit.SampleData
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.time.Duration
import spock.lang.AutoCleanup
import spock.lang.Specification

class ShardDocTiebreakerSpec extends Specification {

  static final EngineInfo OLD_OPENSEARCH = new EngineInfo(EngineProduct.OPENSEARCH, '2.19.0', 'os')

  @AutoCleanup
  ApplicationContext context

  CapturingSearchEngine engine

  ElasticProtosService service

  private void start(String mode, EngineInfo info) {
    Map<String, Object> properties = [
        'test.engine'                   : 'capturing',
        'elastic-protos.bootstrap.enabled': 'false',
    ]
    if (mode != null) {
      properties['elastic-protos.query.shard-doc-tiebreaker'] = mode
    }
    context = ApplicationContext.run(properties)
    engine = context.getBean(CapturingSearchEngine)
    engine.engineInfo(info)
    service = context.getBean(ElasticProtosService)
    service.bootstrap(Firm)
    service.bulkIndex(SampleData.firms())
    engine.clearLog()
  }

  private List<Object> pitSearchSort() {
    String pit = engine.openPit(['firms'], Duration.ofMinutes(1)).id()
    service.search(Criteria.builder().pageSize(2).cursor(pit, '1m', []).build(), Firm)
    engine.lastBody.sort as List<Object>
  }

  private static boolean hasShardDoc(List<Object> sort) {
    sort.any { it instanceof Map && it.containsKey('_shard_doc') }
  }

  def "AUTO is the default and asks the engine only on the first point-in-time search"() {
    given:
    start(null, OLD_OPENSEARCH)

    expect:
    context.getBean(ElasticProtosConfiguration).query.shardDocTiebreaker ==
        ElasticProtosConfiguration.Query.ShardDocTiebreaker.AUTO
    context.getBean(QueryCompilerConfig).shardDocTiebreaker()

    when: 'a search without a cursor'
    service.search(Criteria.builder().pageSize(2).build(), Firm)

    then:
    !engine.log().any { it.startsWith('info') }

    when:
    def first = pitSearchSort()
    def second = pitSearchSort()

    then:
    !hasShardDoc(first)
    !hasShardDoc(second)
    engine.log().count { it.startsWith('info') } == 1
  }

  def "AUTO keeps the tiebreaker on engines that support it"() {
    given:
    start('AUTO', new EngineInfo(EngineProduct.OPENSEARCH, '3.3.0', 'os'))

    expect:
    hasShardDoc(pitSearchSort())
  }

  def "ON forces the tiebreaker without asking the engine"() {
    given:
    start('ON', OLD_OPENSEARCH)

    expect:
    hasShardDoc(pitSearchSort())
    !engine.log().any { it.startsWith('info') }
  }

  def "OFF omits the tiebreaker without asking the engine"() {
    given:
    start('off', new EngineInfo(EngineProduct.ELASTICSEARCH, '9.0.0', 'es'))

    expect:
    !context.getBean(QueryCompilerConfig).shardDocTiebreaker()
    !hasShardDoc(pitSearchSort())
    !engine.log().any { it.startsWith('info') }
  }
}

/** An in-memory engine that keeps the body of the last search. */
class CapturingSearchEngine extends InMemorySearchEngine {

  Map<String, Object> lastBody

  @Override
  SearchResult search(List<String> targets, Map<String, Object> body) {
    lastBody = body
    super.search(targets, body)
  }
}

@Factory
@Requires(property = 'test.engine', value = 'capturing')
class CapturingEngineFactory {

  @Singleton
  @Bean(preDestroy = 'close')
  CapturingSearchEngine capturingSearchEngine() {
    new CapturingSearchEngine()
  }
}
