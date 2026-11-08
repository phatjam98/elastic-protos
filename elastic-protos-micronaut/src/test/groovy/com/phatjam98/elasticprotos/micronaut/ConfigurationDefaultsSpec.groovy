package com.phatjam98.elasticprotos.micronaut

import com.phatjam98.elasticprotos.codec.CodecConfig
import com.phatjam98.elasticprotos.codec.ProtoDocumentCodec
import com.phatjam98.elasticprotos.mapping.IndexNamingStrategy
import com.phatjam98.elasticprotos.mapping.MappingDefaults
import com.phatjam98.elasticprotos.mapping.MappingOverrides
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.migration.MigrationOptions
import com.phatjam98.elasticprotos.migration.MigrationScriptProvider
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.query.QueryCompilerConfig
import io.micronaut.context.ApplicationContext
import java.time.Duration
import spock.lang.AutoCleanup
import spock.lang.Specification

class ConfigurationDefaultsSpec extends Specification {

  @AutoCleanup
  ApplicationContext ctx

  def "every key has the documented default"() {
    given:
    ctx = ApplicationContext.run(['test.engine': 'memory'])
    def config = ctx.getBean(ElasticProtosConfiguration)

    expect:
    config.engine == 'elasticsearch'
    config.hosts == ['http://localhost:9200']
    config.username == null
    config.password == null
    config.apiKey == null
    !config.ssl.insecureTrustAllCertificates
    with(config.bootstrap) {
      enabled
      classes.isEmpty()
      failurePolicy.name() == 'FAIL_FAST'
      allowReindex
      !deleteOld
      removedFields.name() == 'KEEP'
      !dryRun
      clearStaleWriteBlock
      failOnError
      reindexTimeout == Duration.ofMinutes(10)
    }
    config.defaults.shards == 1
    config.defaults.replicas == -1
    config.naming.prefix == ''
    config.codec.includeDefaults
    config.codec.int64AsNumber
    config.bulk.actions == 500
    config.bulk.flushInterval == Duration.ofSeconds(1)
    config.bulk.maxBytes == 5L * 1024 * 1024

    and: 'the beans equal the core defaults'
    ctx.getBean(MappingDefaults) == MappingDefaults.standard()
    ctx.getBean(ProtoDocumentCodec).config() == CodecConfig.standard()
    ctx.getBean(QueryCompilerConfig) == QueryCompilerConfig.standard()
    ctx.getBean(MigrationOptions) == MigrationOptions.standard()
    ctx.getBean(ProtoMapper).map(Firm).alias() == 'firms'
  }

  def "optional naming, overrides and script provider beans are picked up"() {
    given:
    ctx = ApplicationContext.builder()
        .properties(['test.engine': 'memory', 'elastic-protos.naming.prefix': 'ignored'])
        .singletons(
            IndexNamingStrategy.prefixed('custom'),
            MappingOverrides.builder().setting('number_of_shards', 7).build(),
            ({ alias, from, to -> Optional.of('// script') } as MigrationScriptProvider))
        .start()

    when:
    def definition = ctx.getBean(ProtoMapper).map(Firm)

    then:
    definition.alias() == 'custom-firms'
    definition.settings().toString().contains('7')
  }
}
