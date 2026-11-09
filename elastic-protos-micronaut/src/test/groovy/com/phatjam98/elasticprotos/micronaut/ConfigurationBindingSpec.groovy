package com.phatjam98.elasticprotos.micronaut

import com.phatjam98.elasticprotos.codec.ProtoDocumentCodec
import com.phatjam98.elasticprotos.codec.UnknownFieldPolicy
import com.phatjam98.elasticprotos.mapping.MappingDefaults
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.migration.FailurePolicy
import com.phatjam98.elasticprotos.migration.IndexMigrator
import com.phatjam98.elasticprotos.migration.MigrationOptions
import com.phatjam98.elasticprotos.migration.ReindexMode
import com.phatjam98.elasticprotos.migration.RemovedFieldPolicy
import com.phatjam98.elasticprotos.migration.ReplicaPolicy
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.query.QueryCompiler
import com.phatjam98.elasticprotos.query.QueryCompilerConfig
import com.phatjam98.elasticprotos.query.TrackTotalHits
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import java.time.Duration
import spock.lang.Specification

@MicronautTest(startApplication = false)
@Property(name = 'test.engine', value = 'memory')
@Property(name = 'elastic-protos.engine', value = 'opensearch')
@Property(name = 'elastic-protos.hosts', value = 'https://a:9200,https://b:9200')
@Property(name = 'elastic-protos.username', value = 'elastic')
@Property(name = 'elastic-protos.password', value = 'secret')
@Property(name = 'elastic-protos.api-key', value = 'id:key')
@Property(name = 'elastic-protos.ssl.insecure-trust-all-certificates', value = 'true')
@Property(name = 'elastic-protos.bootstrap.enabled', value = 'true')
@Property(name = 'elastic-protos.bootstrap.classes', value = 'com.phatjam98.elasticprotos.proto.example.Firm, com.phatjam98.elasticprotos.proto.example.Document')
@Property(name = 'elastic-protos.bootstrap.failure-policy', value = 'CONTINUE')
@Property(name = 'elastic-protos.bootstrap.allow-reindex', value = 'false')
@Property(name = 'elastic-protos.bootstrap.delete-old', value = 'true')
@Property(name = 'elastic-protos.bootstrap.removed-fields', value = 'reindex')
@Property(name = 'elastic-protos.bootstrap.dry-run', value = 'true')
@Property(name = 'elastic-protos.bootstrap.clear-stale-write-block', value = 'false')
@Property(name = 'elastic-protos.bootstrap.fail-on-error', value = 'false')
@Property(name = 'elastic-protos.bootstrap.reindex-timeout', value = '90s')
@Property(name = 'elastic-protos.bootstrap.reindex-mode', value = 'blocking')
@Property(name = 'elastic-protos.bootstrap.swap-grace', value = '250ms')
@Property(name = 'elastic-protos.bootstrap.backfill', value = 'false')
@Property(name = 'elastic-protos.defaults.shards', value = '3')
@Property(name = 'elastic-protos.defaults.replicas', value = '2')
@Property(name = 'elastic-protos.naming.prefix', value = 'dev')
@Property(name = 'elastic-protos.mapping.strings-as-text', value = 'true')
@Property(name = 'elastic-protos.mapping.keyword-subfield', value = 'false')
@Property(name = 'elastic-protos.mapping.ignore-above', value = '512')
@Property(name = 'elastic-protos.mapping.max-depth', value = '3')
@Property(name = 'elastic-protos.codec.include-defaults', value = 'false')
@Property(name = 'elastic-protos.codec.int64-as-number', value = 'false')
@Property(name = 'elastic-protos.codec.unknown-fields', value = 'FAIL')
@Property(name = 'elastic-protos.query.allow-leading-wildcards', value = 'true')
@Property(name = 'elastic-protos.query.lenient', value = 'true')
@Property(name = 'elastic-protos.query.max-result-window', value = '50000')
@Property(name = 'elastic-protos.query.default-size', value = '25')
@Property(name = 'elastic-protos.query.default-keep-alive', value = '5m')
@Property(name = 'elastic-protos.query.track-total-hits', value = 'false')
@Property(name = 'elastic-protos.query.allow-script-sort', value = 'true')
@Property(name = 'elastic-protos.query.allow-regex', value = 'true')
@Property(name = 'elastic-protos.query.max-page-size', value = '500')
@Property(name = 'elastic-protos.bulk.actions', value = '1000')
@Property(name = 'elastic-protos.bulk.flush-interval', value = '250ms')
@Property(name = 'elastic-protos.bulk.max-bytes', value = '10MB')
class ConfigurationBindingSpec extends Specification {

  @Inject
  ElasticProtosConfiguration config

  @Inject
  MappingDefaults mappingDefaults

  @Inject
  ProtoMapper mapper

  @Inject
  ProtoDocumentCodec codec

  @Inject
  QueryCompilerConfig queryConfig

  @Inject
  QueryCompiler compiler

  @Inject
  MigrationOptions migrationOptions

  @Inject
  IndexMigrator migrator

  def "binds the connection keys"() {
    expect:
    config.engine == 'opensearch'
    config.hosts == ['https://a:9200', 'https://b:9200']
    config.username == 'elastic'
    config.password == 'secret'
    config.apiKey == 'id:key'
    config.ssl.insecureTrustAllCertificates
  }

  def "binds the bootstrap group, including enums and durations"() {
    expect:
    with(config.bootstrap) {
      enabled
      classes*.trim() == [Firm.name, 'com.phatjam98.elasticprotos.proto.example.Document']
      failurePolicy == FailurePolicy.CONTINUE
      !allowReindex
      deleteOld
      removedFields == RemovedFieldPolicy.REINDEX
      dryRun
      !clearStaleWriteBlock
      !failOnError
      reindexTimeout == Duration.ofSeconds(90)
      reindexMode == ReindexMode.BLOCKING
      swapGrace == Duration.ofMillis(250)
      !backfill
    }
  }

  def "binds defaults, naming and mapping"() {
    expect:
    config.defaults.shards == 3
    config.defaults.replicas == 2
    config.naming.prefix == 'dev'
    with(config.mapping) {
      stringsAsText
      !keywordSubfield
      ignoreAbove == 512
      maxDepth == 3
    }
  }

  def "binds codec, query and bulk"() {
    expect:
    !config.codec.includeDefaults
    !config.codec.int64AsNumber
    config.codec.unknownFields == UnknownFieldPolicy.FAIL
    with(config.query) {
      allowLeadingWildcards
      lenient
      maxResultWindow == 50000
      defaultSize == 25
      defaultKeepAlive == Duration.ofMinutes(5)
      trackTotalHits == TrackTotalHits.none()
      allowScriptSort
      allowRegex
      maxPageSize == 500
    }
    config.bulk.actions == 1000
    config.bulk.flushInterval == Duration.ofMillis(250)
    config.bulk.maxBytes == 10L * 1024 * 1024
  }

  def "the component beans follow the configuration"() {
    expect:
    mappingDefaults == MappingDefaults.standard().withStringsAsText(true).withKeywordSubfield(false)
        .withIgnoreAbove(512).withMaxDepth(3)
    mapper.defaults() == mappingDefaults
    mapper.map(Firm).alias() == 'dev-firms'
    mapper.map(Firm).name().startsWith('dev-firms-v')
    !codec.config().includeDefaults()
    !codec.config().int64AsNumber()
    codec.config().unknownFields() == UnknownFieldPolicy.FAIL
    queryConfig == QueryCompilerConfig.standard().withAllowLeadingWildcards(true).withLenient(true)
        .withMaxResultWindow(50000).withDefaultSize(25).withDefaultKeepAlive(Duration.ofMinutes(5))
        .withTrackTotalHits(TrackTotalHits.none()).withAllowScriptSort(true).withAllowRegex(true).withMaxPageSize(500)
    compiler.config() == queryConfig
    migrationOptions == MigrationOptions.builder().failure(FailurePolicy.CONTINUE).allowReindex(false)
        .deleteOldAfter(true).removedFields(RemovedFieldPolicy.REINDEX).defaultShards(3)
        .replicas(ReplicaPolicy.fixed(2)).reindexTimeout(Duration.ofSeconds(90)).dryRun(true)
        .clearStaleWriteBlock(false).reindexMode(ReindexMode.BLOCKING).swapGrace(Duration.ofMillis(250))
        .backfill(false).build()
    migrator.options() == migrationOptions
  }
}
