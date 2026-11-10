package com.phatjam98.elasticprotos.micronaut;

import com.phatjam98.elasticprotos.codec.CodecConfig;
import com.phatjam98.elasticprotos.codec.ProtoDocumentCodec;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.mapping.IndexNamingStrategy;
import com.phatjam98.elasticprotos.mapping.MappingDefaults;
import com.phatjam98.elasticprotos.mapping.MappingOverrides;
import com.phatjam98.elasticprotos.mapping.ProtoMapper;
import com.phatjam98.elasticprotos.micronaut.ElasticProtosConfiguration.Query.ShardDocTiebreaker;
import com.phatjam98.elasticprotos.migration.IndexMigrator;
import com.phatjam98.elasticprotos.migration.MigrationOptions;
import com.phatjam98.elasticprotos.migration.MigrationScriptProvider;
import com.phatjam98.elasticprotos.migration.ReplicaPolicy;
import com.phatjam98.elasticprotos.query.QueryCompiler;
import com.phatjam98.elasticprotos.query.QueryCompilerConfig;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Nullable;
import jakarta.inject.Singleton;
import java.util.stream.Collectors;

/**
 * Creates the elastic-protos components from {@link ElasticProtosConfiguration}. Every bean is
 * created only when a {@link SearchEngine} bean exists.
 *
 * <p>Optional beans the application may define to customize them: {@link IndexNamingStrategy}
 * (wins over {@code elastic-protos.naming.prefix}), {@link MappingOverrides} and {@link
 * MigrationScriptProvider} (default: scripts under {@code migrations/} on the classpath). To
 * replace any bean here entirely, annotate yours with {@code @Replaces}.
 */
@Factory
@Requires(beans = SearchEngine.class)
public class ElasticProtosFactory {

  /**
   * Returns the mapping defaults from {@code elastic-protos.mapping.*}.
   *
   * @param config the configuration
   * @return the defaults
   */
  @Singleton
  public MappingDefaults mappingDefaults(ElasticProtosConfiguration config) {
    return mappingDefaultsOf(config);
  }

  static MappingDefaults mappingDefaultsOf(ElasticProtosConfiguration config) {
    ElasticProtosConfiguration.Mapping mapping = config.getMapping();
    return MappingDefaults.builder()
        .stringsAsText(mapping.isStringsAsText())
        .keywordSubfield(mapping.isKeywordSubfield())
        .ignoreAbove(mapping.getIgnoreAbove())
        .maxDepth(mapping.getMaxDepth())
        .build();
  }

  /**
   * Returns the mapper.
   *
   * @param config the configuration
   * @param defaults the mapping defaults
   * @param naming an optional naming strategy bean
   * @param overrides an optional overrides bean
   * @return the mapper
   */
  @Singleton
  public ProtoMapper protoMapper(ElasticProtosConfiguration config, MappingDefaults defaults,
      @Nullable IndexNamingStrategy naming, @Nullable MappingOverrides overrides) {
    IndexNamingStrategy strategy = naming;
    if (strategy == null) {
      String prefix = config.getNaming().getPrefix();
      strategy = prefix == null || prefix.isBlank()
          ? IndexNamingStrategy.DEFAULT
          : IndexNamingStrategy.prefixed(prefix.trim());
    }
    return ProtoMapper.builder()
        .defaults(defaults)
        .naming(strategy)
        .overrides(overrides == null ? MappingOverrides.none() : overrides)
        .build();
  }

  /**
   * Returns the document codec configured from {@code elastic-protos.codec.*}.
   *
   * @param config the configuration
   * @param mapper the mapper
   * @return the codec
   */
  @Singleton
  public ProtoDocumentCodec protoDocumentCodec(ElasticProtosConfiguration config,
      ProtoMapper mapper) {
    return ProtoDocumentCodec.of(mapper, codecConfigOf(config));
  }

  static CodecConfig codecConfigOf(ElasticProtosConfiguration config) {
    ElasticProtosConfiguration.Codec codec = config.getCodec();
    return CodecConfig.builder()
        .includeDefaults(codec.isIncludeDefaults())
        .int64AsNumber(codec.isInt64AsNumber())
        .unknownFields(codec.getUnknownFields())
        .build();
  }

  /**
   * Returns the query compiler settings from {@code elastic-protos.query.*}. The {@code _shard_doc}
   * tiebreaker is on unless {@code shard-doc-tiebreaker} is {@code OFF}; with {@code AUTO}, {@link
   * ElasticProtosService} turns it off for searches on engines without {@code _shard_doc}.
   *
   * @param config the configuration
   * @return the settings
   */
  @Singleton
  public QueryCompilerConfig queryCompilerConfig(ElasticProtosConfiguration config) {
    return queryCompilerConfigOf(config);
  }

  static QueryCompilerConfig queryCompilerConfigOf(ElasticProtosConfiguration config) {
    ElasticProtosConfiguration.Query query = config.getQuery();
    return QueryCompilerConfig.builder()
        .allowLeadingWildcards(query.isAllowLeadingWildcards())
        .lenient(query.isLenient())
        .maxResultWindow(query.getMaxResultWindow())
        .defaultSize(query.getDefaultSize())
        .defaultKeepAlive(query.getDefaultKeepAlive())
        .trackTotalHits(query.getTrackTotalHits())
        .allowScriptSort(query.isAllowScriptSort())
        .allowRegex(query.isAllowRegex())
        .maxPageSize(query.getMaxPageSize())
        .maxTermsSize(query.getMaxTermsSize())
        .shardDocTiebreaker(query.getShardDocTiebreaker() != ShardDocTiebreaker.OFF)
        .build();
  }

  /**
   * Returns the query compiler.
   *
   * @param config the compiler settings
   * @return the compiler
   */
  @Singleton
  public QueryCompiler queryCompiler(QueryCompilerConfig config) {
    return QueryCompiler.of(config);
  }

  /**
   * Returns the migration options from {@code elastic-protos.bootstrap.*} and {@code
   * elastic-protos.defaults.*}.
   *
   * @param config the configuration
   * @return the options
   */
  @Singleton
  public MigrationOptions migrationOptions(ElasticProtosConfiguration config) {
    return migrationOptionsOf(config);
  }

  static MigrationOptions migrationOptionsOf(ElasticProtosConfiguration config) {
    ElasticProtosConfiguration.Bootstrap bootstrap = config.getBootstrap();
    ElasticProtosConfiguration.Defaults defaults = config.getDefaults();
    ReplicaPolicy replicas = defaults.getReplicas() < 0
        ? ReplicaPolicy.dataNodesMinusOne(1)
        : ReplicaPolicy.fixed(defaults.getReplicas());
    return MigrationOptions.builder()
        .failure(bootstrap.getFailurePolicy())
        .allowReindex(bootstrap.isAllowReindex())
        .deleteOldAfter(bootstrap.isDeleteOld())
        .removedFields(bootstrap.getRemovedFields())
        .defaultShards(defaults.getShards())
        .replicas(replicas)
        .reindexTimeout(bootstrap.getReindexTimeout())
        .dryRun(bootstrap.isDryRun())
        .clearStaleWriteBlock(bootstrap.isClearStaleWriteBlock())
        .reindexMode(bootstrap.getReindexMode())
        .swapGrace(bootstrap.getSwapGrace())
        .backfill(bootstrap.isBackfill())
        .lock(bootstrap.getLock().isEnabled())
        .lockWait(bootstrap.getLock().getWait())
        .lockTtl(bootstrap.getLock().getTtl())
        .staticSettings(bootstrap.getStaticSettings())
        .extraDynamicSettings(bootstrap.getExtraDynamicSettings().stream().map(String::trim)
            .filter(key -> !key.isEmpty()).collect(Collectors.toSet()))
        .build();
  }

  /**
   * Returns the index migrator.
   *
   * @param engine the engine
   * @param mapper the mapper
   * @param options the migration options
   * @param scripts an optional script provider bean; defaults to the classpath provider
   * @return the migrator
   */
  @Singleton
  public IndexMigrator indexMigrator(SearchEngine engine, ProtoMapper mapper,
      MigrationOptions options, @Nullable MigrationScriptProvider scripts) {
    return IndexMigrator.builder()
        .engine(engine)
        .mapper(mapper)
        .options(options)
        .scriptProvider(scripts == null ? MigrationScriptProvider.classpath() : scripts)
        .build();
  }
}
