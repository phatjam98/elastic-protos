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
    ElasticProtosConfiguration.Mapping mapping = config.getMapping();
    return MappingDefaults.standard()
        .withStringsAsText(mapping.isStringsAsText())
        .withKeywordSubfield(mapping.isKeywordSubfield())
        .withIgnoreAbove(mapping.getIgnoreAbove())
        .withMaxDepth(mapping.getMaxDepth());
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
    ElasticProtosConfiguration.Codec codec = config.getCodec();
    return ProtoDocumentCodec.of(mapper, CodecConfig.standard()
        .withIncludeDefaults(codec.isIncludeDefaults())
        .withInt64AsNumber(codec.isInt64AsNumber())
        .withUnknownFields(codec.getUnknownFields()));
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
    ElasticProtosConfiguration.Query query = config.getQuery();
    return QueryCompilerConfig.standard()
        .withAllowLeadingWildcards(query.isAllowLeadingWildcards())
        .withLenient(query.isLenient())
        .withMaxResultWindow(query.getMaxResultWindow())
        .withDefaultSize(query.getDefaultSize())
        .withDefaultKeepAlive(query.getDefaultKeepAlive())
        .withTrackTotalHits(query.isTrackTotalHits())
        .withShardDocTiebreaker(query.getShardDocTiebreaker() != ShardDocTiebreaker.OFF);
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
    ElasticProtosConfiguration.Bootstrap bootstrap = config.getBootstrap();
    ElasticProtosConfiguration.Defaults defaults = config.getDefaults();
    ReplicaPolicy replicas = defaults.getReplicas() < 0
        ? ReplicaPolicy.dataNodesMinusOne(1)
        : ReplicaPolicy.fixed(defaults.getReplicas());
    return new MigrationOptions(
        bootstrap.getFailurePolicy(),
        bootstrap.isAllowReindex(),
        bootstrap.isDeleteOld(),
        bootstrap.getRemovedFields(),
        defaults.getShards(),
        replicas,
        bootstrap.getReindexTimeout(),
        bootstrap.isDryRun());
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
