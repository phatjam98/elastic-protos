package com.phatjam98.elasticprotos.micronaut;

import com.phatjam98.elasticprotos.codec.CodecConfig;
import com.phatjam98.elasticprotos.codec.UnknownFieldPolicy;
import com.phatjam98.elasticprotos.mapping.MappingDefaults;
import com.phatjam98.elasticprotos.migration.FailurePolicy;
import com.phatjam98.elasticprotos.migration.ReindexMode;
import com.phatjam98.elasticprotos.migration.RemovedFieldPolicy;
import com.phatjam98.elasticprotos.query.QueryCompilerConfig;
import com.phatjam98.elasticprotos.query.TrackTotalHits;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Context;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.convert.format.ReadableBytes;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Configuration of the elastic-protos Micronaut integration, bound from {@code elastic-protos.*}.
 *
 * <p>Connection keys:
 *
 * <ul>
 *   <li>{@code elastic-protos.engine}: {@code elasticsearch} (default) or {@code opensearch}
 *   <li>{@code elastic-protos.hosts}: list of URLs, default {@code http://localhost:9200}
 *   <li>{@code elastic-protos.username}, {@code elastic-protos.password}: basic authentication
 *   <li>{@code elastic-protos.api-key}: API key, base64 or {@code id:key}
 *   <li>{@code elastic-protos.ssl.insecure-trust-all-certificates}: default false; never in
 *       production
 * </ul>
 *
 * <p>The nested groups {@link Bootstrap bootstrap}, {@link Defaults defaults}, {@link Naming
 * naming}, {@link Mapping mapping}, {@link Codec codec}, {@link Query query} and {@link Bulk bulk}
 * document their own keys.
 *
 * <p>The configuration is a {@code @Context} bean: it is bound and {@linkplain #validate()
 * validated} while the application context starts, so an invalid value aborts startup even when
 * no elastic-protos bean is ever requested.
 */
@Context
@ConfigurationProperties(ElasticProtosConfiguration.PREFIX)
public class ElasticProtosConfiguration {

  /** The configuration prefix. */
  public static final String PREFIX = "elastic-protos";

  /** Value of {@code elastic-protos.engine} selecting the Elasticsearch adapter. */
  public static final String ELASTICSEARCH = "elasticsearch";

  /** Value of {@code elastic-protos.engine} selecting the OpenSearch adapter. */
  public static final String OPENSEARCH = "opensearch";

  private String engine = ELASTICSEARCH;
  private List<String> hosts = new ArrayList<>(List.of("http://localhost:9200"));
  private String username;
  private String password;
  private String apiKey;
  private Ssl ssl = new Ssl();
  private Bootstrap bootstrap = new Bootstrap();
  private Defaults defaults = new Defaults();
  private Naming naming = new Naming();
  private Mapping mapping = new Mapping();
  private Codec codec = new Codec();
  private Query query = new Query();
  private Bulk bulk = new Bulk();

  /**
   * Checks at startup that the settings make valid mapping, codec, query and migration settings,
   * for example that {@code query.default-size} does not exceed {@code query.max-page-size}. The
   * configuration is a {@code @Context} bean, so an invalid value stops {@code
   * ApplicationContext.start()} even when no bean that uses it is ever requested.
   *
   * @throws NullPointerException when a required setting is null
   * @throws IllegalArgumentException when a setting is out of range
   */
  @PostConstruct
  public void validate() {
    ElasticProtosFactory.mappingDefaultsOf(this);
    ElasticProtosFactory.codecConfigOf(this);
    ElasticProtosFactory.queryCompilerConfigOf(this);
    ElasticProtosFactory.migrationOptionsOf(this);
  }

  /** Returns {@code elastic-protos.engine}: {@code elasticsearch} or {@code opensearch}. */
  public String getEngine() {
    return engine;
  }

  /** Sets {@code elastic-protos.engine}. */
  public void setEngine(String engine) {
    this.engine = engine;
  }

  /** Returns {@code elastic-protos.hosts}. */
  public List<String> getHosts() {
    return hosts;
  }

  /** Sets {@code elastic-protos.hosts}. */
  public void setHosts(List<String> hosts) {
    this.hosts = hosts;
  }

  /** Returns {@code elastic-protos.username}, or null. */
  @Nullable
  public String getUsername() {
    return username;
  }

  /** Sets {@code elastic-protos.username}. */
  public void setUsername(@Nullable String username) {
    this.username = username;
  }

  /** Returns {@code elastic-protos.password}, or null. */
  @Nullable
  public String getPassword() {
    return password;
  }

  /** Sets {@code elastic-protos.password}. */
  public void setPassword(@Nullable String password) {
    this.password = password;
  }

  /** Returns {@code elastic-protos.api-key}, or null. */
  @Nullable
  public String getApiKey() {
    return apiKey;
  }

  /** Sets {@code elastic-protos.api-key}. */
  public void setApiKey(@Nullable String apiKey) {
    this.apiKey = apiKey;
  }

  /** Returns the {@code elastic-protos.ssl} group. */
  public Ssl getSsl() {
    return ssl;
  }

  /** Sets the {@code elastic-protos.ssl} group. */
  public void setSsl(Ssl ssl) {
    this.ssl = ssl;
  }

  /** Returns the {@code elastic-protos.bootstrap} group. */
  public Bootstrap getBootstrap() {
    return bootstrap;
  }

  /** Sets the {@code elastic-protos.bootstrap} group. */
  public void setBootstrap(Bootstrap bootstrap) {
    this.bootstrap = bootstrap;
  }

  /** Returns the {@code elastic-protos.defaults} group. */
  public Defaults getDefaults() {
    return defaults;
  }

  /** Sets the {@code elastic-protos.defaults} group. */
  public void setDefaults(Defaults defaults) {
    this.defaults = defaults;
  }

  /** Returns the {@code elastic-protos.naming} group. */
  public Naming getNaming() {
    return naming;
  }

  /** Sets the {@code elastic-protos.naming} group. */
  public void setNaming(Naming naming) {
    this.naming = naming;
  }

  /** Returns the {@code elastic-protos.mapping} group. */
  public Mapping getMapping() {
    return mapping;
  }

  /** Sets the {@code elastic-protos.mapping} group. */
  public void setMapping(Mapping mapping) {
    this.mapping = mapping;
  }

  /** Returns the {@code elastic-protos.codec} group. */
  public Codec getCodec() {
    return codec;
  }

  /** Sets the {@code elastic-protos.codec} group. */
  public void setCodec(Codec codec) {
    this.codec = codec;
  }

  /** Returns the {@code elastic-protos.query} group. */
  public Query getQuery() {
    return query;
  }

  /** Sets the {@code elastic-protos.query} group. */
  public void setQuery(Query query) {
    this.query = query;
  }

  /** Returns the {@code elastic-protos.bulk} group. */
  public Bulk getBulk() {
    return bulk;
  }

  /** Sets the {@code elastic-protos.bulk} group. */
  public void setBulk(Bulk bulk) {
    this.bulk = bulk;
  }

  /**
   * TLS settings, bound from {@code elastic-protos.ssl.*}.
   *
   * <ul>
   *   <li>{@code insecure-trust-all-certificates}: disable certificate and hostname checks;
   *       default false. For local development only.
   * </ul>
   *
   * <p>For a private CA, define a {@code javax.net.ssl.SSLContext} bean named {@code
   * elastic-protos}; the engine factory uses it.
   */
  @ConfigurationProperties("ssl")
  public static class Ssl {

    private boolean insecureTrustAllCertificates;

    /** Returns {@code insecure-trust-all-certificates}. */
    public boolean isInsecureTrustAllCertificates() {
      return insecureTrustAllCertificates;
    }

    /** Sets {@code insecure-trust-all-certificates}. */
    public void setInsecureTrustAllCertificates(boolean insecureTrustAllCertificates) {
      this.insecureTrustAllCertificates = insecureTrustAllCertificates;
    }
  }

  /**
   * Startup index bootstrap, bound from {@code elastic-protos.bootstrap.*}.
   *
   * <ul>
   *   <li>{@code enabled}: run the bootstrap at startup; default true
   *   <li>{@code classes}: fully-qualified generated message class names; default empty, which
   *       skips the bootstrap
   *   <li>{@code failure-policy}: {@code FAIL_FAST} (default) or {@code CONTINUE}
   *   <li>{@code allow-reindex}: apply breaking mapping changes by reindexing; default true
   *   <li>{@code delete-old}: delete the old index after a successful reindex; default false
   *   <li>{@code removed-fields}: {@code KEEP} (default) or {@code REINDEX}
   *   <li>{@code dry-run}: plan and log only; default false
   *   <li>{@code clear-stale-write-block}: lift a write block found on an alias target that needs
   *       no reindex, assuming an interrupted reindex left it; default true
   *   <li>{@code fail-on-error}: abort startup when an index fails or is blocked; default true
   *   <li>{@code reindex-timeout}: how long a reindex may take; default {@code 10m}
   *   <li>{@code reindex-mode}: {@code ZERO_DOWNTIME} (default), which keeps the live index
   *       writable during a reindex, or {@code BLOCKING}
   *   <li>{@code swap-grace}: how long a zero-downtime reindex waits after the alias swap before
   *       it write-blocks the old index; default {@code 5s}
   *   <li>{@code backfill}: apply additions existing documents were never indexed into in place
   *       with an update by query instead of reindexing; default true
   * </ul>
   */
  @ConfigurationProperties("bootstrap")
  public static class Bootstrap {

    private boolean enabled = true;
    private List<String> classes = new ArrayList<>();
    private FailurePolicy failurePolicy = FailurePolicy.FAIL_FAST;
    private boolean allowReindex = true;
    private boolean deleteOld;
    private RemovedFieldPolicy removedFields = RemovedFieldPolicy.KEEP;
    private boolean dryRun;
    private boolean clearStaleWriteBlock = true;
    private boolean failOnError = true;
    private Duration reindexTimeout = Duration.ofMinutes(10);
    private ReindexMode reindexMode = ReindexMode.ZERO_DOWNTIME;
    private Duration swapGrace = Duration.ofSeconds(5);
    private boolean backfill = true;

    /** Returns {@code enabled}. */
    public boolean isEnabled() {
      return enabled;
    }

    /** Sets {@code enabled}. */
    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    /** Returns {@code classes}. */
    public List<String> getClasses() {
      return classes;
    }

    /** Sets {@code classes}. */
    public void setClasses(List<String> classes) {
      this.classes = classes;
    }

    /** Returns {@code failure-policy}. */
    public FailurePolicy getFailurePolicy() {
      return failurePolicy;
    }

    /** Sets {@code failure-policy}. */
    public void setFailurePolicy(FailurePolicy failurePolicy) {
      this.failurePolicy = failurePolicy;
    }

    /** Returns {@code allow-reindex}. */
    public boolean isAllowReindex() {
      return allowReindex;
    }

    /** Sets {@code allow-reindex}. */
    public void setAllowReindex(boolean allowReindex) {
      this.allowReindex = allowReindex;
    }

    /** Returns {@code delete-old}. */
    public boolean isDeleteOld() {
      return deleteOld;
    }

    /** Sets {@code delete-old}. */
    public void setDeleteOld(boolean deleteOld) {
      this.deleteOld = deleteOld;
    }

    /** Returns {@code removed-fields}. */
    public RemovedFieldPolicy getRemovedFields() {
      return removedFields;
    }

    /** Sets {@code removed-fields}. */
    public void setRemovedFields(RemovedFieldPolicy removedFields) {
      this.removedFields = removedFields;
    }

    /** Returns {@code dry-run}. */
    public boolean isDryRun() {
      return dryRun;
    }

    /** Sets {@code dry-run}. */
    public void setDryRun(boolean dryRun) {
      this.dryRun = dryRun;
    }

    /** Returns {@code clear-stale-write-block}. */
    public boolean isClearStaleWriteBlock() {
      return clearStaleWriteBlock;
    }

    /** Sets {@code clear-stale-write-block}. */
    public void setClearStaleWriteBlock(boolean clearStaleWriteBlock) {
      this.clearStaleWriteBlock = clearStaleWriteBlock;
    }

    /** Returns {@code fail-on-error}. */
    public boolean isFailOnError() {
      return failOnError;
    }

    /** Sets {@code fail-on-error}. */
    public void setFailOnError(boolean failOnError) {
      this.failOnError = failOnError;
    }

    /** Returns {@code reindex-timeout}. */
    public Duration getReindexTimeout() {
      return reindexTimeout;
    }

    /** Sets {@code reindex-timeout}. */
    public void setReindexTimeout(Duration reindexTimeout) {
      this.reindexTimeout = reindexTimeout;
    }

    /** Returns {@code reindex-mode}. */
    public ReindexMode getReindexMode() {
      return reindexMode;
    }

    /** Sets {@code reindex-mode}. */
    public void setReindexMode(ReindexMode reindexMode) {
      this.reindexMode = reindexMode;
    }

    /** Returns {@code swap-grace}. */
    public Duration getSwapGrace() {
      return swapGrace;
    }

    /** Sets {@code swap-grace}. */
    public void setSwapGrace(Duration swapGrace) {
      this.swapGrace = swapGrace;
    }

    /** Returns {@code backfill}. */
    public boolean isBackfill() {
      return backfill;
    }

    /** Sets {@code backfill}. */
    public void setBackfill(boolean backfill) {
      this.backfill = backfill;
    }
  }

  /**
   * Settings for newly created indices, bound from {@code elastic-protos.defaults.*}. Settings
   * declared by a message's options win.
   *
   * <ul>
   *   <li>{@code shards}: shard count; default 1
   *   <li>{@code replicas}: replica count; default -1, meaning data nodes minus one, at most 1;
   *       any value of 0 or more is used as is
   * </ul>
   */
  @ConfigurationProperties("defaults")
  public static class Defaults {

    private int shards = 1;
    private int replicas = -1;

    /** Returns {@code shards}. */
    public int getShards() {
      return shards;
    }

    /** Sets {@code shards}. */
    public void setShards(int shards) {
      this.shards = shards;
    }

    /** Returns {@code replicas}; negative means data nodes minus one, at most 1. */
    public int getReplicas() {
      return replicas;
    }

    /** Sets {@code replicas}. */
    public void setReplicas(int replicas) {
      this.replicas = replicas;
    }
  }

  /**
   * Index naming, bound from {@code elastic-protos.naming.*}.
   *
   * <ul>
   *   <li>{@code prefix}: prefix for every alias and index, joined with {@code -}, for example
   *       per environment; default empty. An {@code IndexNamingStrategy} bean overrides it.
   * </ul>
   */
  @ConfigurationProperties("naming")
  public static class Naming {

    private String prefix = "";

    /** Returns {@code prefix}. */
    public String getPrefix() {
      return prefix;
    }

    /** Sets {@code prefix}. */
    public void setPrefix(String prefix) {
      this.prefix = prefix;
    }
  }

  /**
   * Mapping defaults, bound from {@code elastic-protos.mapping.*}; the defaults are those of
   * {@link MappingDefaults#standard()}.
   *
   * <ul>
   *   <li>{@code strings-as-text}: map plain strings to {@code text}; default false
   *   <li>{@code keyword-subfield}: give text fields a {@code .keyword} sub-field; default true
   *   <li>{@code ignore-above}: {@code ignore_above} of keyword fields; default 256
   *   <li>{@code max-depth}: how deep nested messages are expanded; default 5
   * </ul>
   */
  @ConfigurationProperties("mapping")
  public static class Mapping {

    private static final MappingDefaults STANDARD = MappingDefaults.standard();

    private boolean stringsAsText = STANDARD.stringsAsText();
    private boolean keywordSubfield = STANDARD.keywordSubfield();
    private int ignoreAbove = STANDARD.ignoreAbove();
    private int maxDepth = STANDARD.maxDepth();

    /** Returns {@code strings-as-text}. */
    public boolean isStringsAsText() {
      return stringsAsText;
    }

    /** Sets {@code strings-as-text}. */
    public void setStringsAsText(boolean stringsAsText) {
      this.stringsAsText = stringsAsText;
    }

    /** Returns {@code keyword-subfield}. */
    public boolean isKeywordSubfield() {
      return keywordSubfield;
    }

    /** Sets {@code keyword-subfield}. */
    public void setKeywordSubfield(boolean keywordSubfield) {
      this.keywordSubfield = keywordSubfield;
    }

    /** Returns {@code ignore-above}. */
    public int getIgnoreAbove() {
      return ignoreAbove;
    }

    /** Sets {@code ignore-above}. */
    public void setIgnoreAbove(int ignoreAbove) {
      this.ignoreAbove = ignoreAbove;
    }

    /** Returns {@code max-depth}. */
    public int getMaxDepth() {
      return maxDepth;
    }

    /** Sets {@code max-depth}. */
    public void setMaxDepth(int maxDepth) {
      this.maxDepth = maxDepth;
    }
  }

  /**
   * Document codec settings, bound from {@code elastic-protos.codec.*}; the defaults are those of
   * {@link CodecConfig#standard()}.
   *
   * <ul>
   *   <li>{@code include-defaults}: write proto3 default values (false, 0, "", first enum value,
   *       empty lists and maps) so EQUALS, ranges and sorts see them; default true
   *   <li>{@code int64-as-number}: write 64-bit integers as JSON numbers; default true
   *   <li>{@code unknown-fields}: {@code IGNORE} (default) or {@code FAIL} when reading back
   * </ul>
   */
  @ConfigurationProperties("codec")
  public static class Codec {

    private static final CodecConfig STANDARD = CodecConfig.standard();

    private boolean includeDefaults = STANDARD.includeDefaults();
    private boolean int64AsNumber = STANDARD.int64AsNumber();
    private UnknownFieldPolicy unknownFields = STANDARD.unknownFields();

    /** Returns {@code include-defaults}. */
    public boolean isIncludeDefaults() {
      return includeDefaults;
    }

    /** Sets {@code include-defaults}. */
    public void setIncludeDefaults(boolean includeDefaults) {
      this.includeDefaults = includeDefaults;
    }

    /** Returns {@code int64-as-number}. */
    public boolean isInt64AsNumber() {
      return int64AsNumber;
    }

    /** Sets {@code int64-as-number}. */
    public void setInt64AsNumber(boolean int64AsNumber) {
      this.int64AsNumber = int64AsNumber;
    }

    /** Returns {@code unknown-fields}. */
    public UnknownFieldPolicy getUnknownFields() {
      return unknownFields;
    }

    /** Sets {@code unknown-fields}. */
    public void setUnknownFields(UnknownFieldPolicy unknownFields) {
      this.unknownFields = unknownFields;
    }
  }

  /**
   * Query compiler settings, bound from {@code elastic-protos.query.*}; the defaults are those of
   * {@link QueryCompilerConfig#standard()}.
   *
   * <ul>
   *   <li>{@code allow-leading-wildcards}: allow wildcard queries starting with {@code *}; default
   *       false
   *   <li>{@code lenient}: skip unmapped fields with a warning instead of failing; default false
   *   <li>{@code max-result-window}: the largest {@code from + size} without a point in time;
   *       default 10000
   *   <li>{@code default-size}: page size when the criteria set none; default 10
   *   <li>{@code default-keep-alive}: point-in-time keep-alive; default {@code 1m}
   *   <li>{@code track-total-hits}: {@code true} counts hits exactly, {@code false} counts none,
   *       a number counts up to that bound, {@code default} leaves the engine's default; default
   *       true
   *   <li>{@code allow-script-sort}: accept script sorts, which run a caller-supplied Painless
   *       script on the cluster; default false
   *   <li>{@code allow-regex}: accept {@code TEXT_REGEX}; default false
   *   <li>{@code max-page-size}: the largest page size, with or without a point in time; default
   *       10000
   * </ul>
   */
  @ConfigurationProperties("query")
  public static class Query {

    private static final QueryCompilerConfig STANDARD = QueryCompilerConfig.standard();

    private boolean allowLeadingWildcards = STANDARD.allowLeadingWildcards();
    private boolean lenient = STANDARD.lenient();
    private int maxResultWindow = STANDARD.maxResultWindow();
    private int defaultSize = STANDARD.defaultSize();
    private Duration defaultKeepAlive = STANDARD.defaultKeepAlive();
    private TrackTotalHits trackTotalHits = STANDARD.trackTotalHits();
    private ShardDocTiebreaker shardDocTiebreaker = ShardDocTiebreaker.AUTO;
    private boolean allowScriptSort = STANDARD.allowScriptSort();
    private boolean allowRegex = STANDARD.allowRegex();
    private int maxPageSize = STANDARD.maxPageSize();

    /** Values of {@code shard-doc-tiebreaker}. */
    public enum ShardDocTiebreaker {
      /**
       * Use the {@code _shard_doc} tiebreaker only if the engine supports it; asked once, on the
       * first point-in-time search.
       */
      AUTO,
      /** Always append the {@code _shard_doc} tiebreaker to point-in-time searches. */
      ON,
      /** Never append the {@code _shard_doc} tiebreaker. */
      OFF
    }

    /** Returns {@code allow-leading-wildcards}. */
    public boolean isAllowLeadingWildcards() {
      return allowLeadingWildcards;
    }

    /** Sets {@code allow-leading-wildcards}. */
    public void setAllowLeadingWildcards(boolean allowLeadingWildcards) {
      this.allowLeadingWildcards = allowLeadingWildcards;
    }

    /** Returns {@code lenient}. */
    public boolean isLenient() {
      return lenient;
    }

    /** Sets {@code lenient}. */
    public void setLenient(boolean lenient) {
      this.lenient = lenient;
    }

    /** Returns {@code max-result-window}. */
    public int getMaxResultWindow() {
      return maxResultWindow;
    }

    /** Sets {@code max-result-window}. */
    public void setMaxResultWindow(int maxResultWindow) {
      this.maxResultWindow = maxResultWindow;
    }

    /** Returns {@code default-size}. */
    public int getDefaultSize() {
      return defaultSize;
    }

    /** Sets {@code default-size}. */
    public void setDefaultSize(int defaultSize) {
      this.defaultSize = defaultSize;
    }

    /** Returns {@code default-keep-alive}. */
    public Duration getDefaultKeepAlive() {
      return defaultKeepAlive;
    }

    /** Sets {@code default-keep-alive}. */
    public void setDefaultKeepAlive(Duration defaultKeepAlive) {
      this.defaultKeepAlive = defaultKeepAlive;
    }

    /** Returns {@code track-total-hits}. */
    public TrackTotalHits getTrackTotalHits() {
      return trackTotalHits;
    }

    /**
     * Sets {@code track-total-hits}; configuration values {@code true}, {@code false}, {@code
     * default} and numbers are converted by {@link TrackTotalHitsConverter}.
     *
     * @throws NullPointerException when {@code trackTotalHits} is null
     */
    public void setTrackTotalHits(TrackTotalHits trackTotalHits) {
      this.trackTotalHits = Objects.requireNonNull(trackTotalHits, "trackTotalHits");
    }

    /** Returns {@code shard-doc-tiebreaker}. */
    public ShardDocTiebreaker getShardDocTiebreaker() {
      return shardDocTiebreaker;
    }

    /** Sets {@code shard-doc-tiebreaker}. */
    public void setShardDocTiebreaker(ShardDocTiebreaker shardDocTiebreaker) {
      this.shardDocTiebreaker = shardDocTiebreaker;
    }

    /** Returns {@code allow-script-sort}. */
    public boolean isAllowScriptSort() {
      return allowScriptSort;
    }

    /** Sets {@code allow-script-sort}. */
    public void setAllowScriptSort(boolean allowScriptSort) {
      this.allowScriptSort = allowScriptSort;
    }

    /** Returns {@code allow-regex}. */
    public boolean isAllowRegex() {
      return allowRegex;
    }

    /** Sets {@code allow-regex}. */
    public void setAllowRegex(boolean allowRegex) {
      this.allowRegex = allowRegex;
    }

    /** Returns {@code max-page-size}. */
    public int getMaxPageSize() {
      return maxPageSize;
    }

    /** Sets {@code max-page-size}. */
    public void setMaxPageSize(int maxPageSize) {
      this.maxPageSize = maxPageSize;
    }
  }

  /**
   * Settings of the {@code BulkBuffer}s handed out by {@link ElasticProtosService#bulkBuffer()},
   * bound from {@code elastic-protos.bulk.*}.
   *
   * <ul>
   *   <li>{@code actions}: send a batch at this many operations; default 500
   *   <li>{@code flush-interval}: also send at this interval, {@code 0s} to disable; default
   *       {@code 1s}
   *   <li>{@code max-bytes}: send a batch at about this size; accepts {@code 5MB}-style values;
   *       default 5 MiB
   * </ul>
   */
  @ConfigurationProperties("bulk")
  public static class Bulk {

    private int actions = 500;
    private Duration flushInterval = Duration.ofSeconds(1);
    private long maxBytes = 5L * 1024 * 1024;

    /** Returns {@code actions}. */
    public int getActions() {
      return actions;
    }

    /** Sets {@code actions}. */
    public void setActions(int actions) {
      this.actions = actions;
    }

    /** Returns {@code flush-interval}. */
    public Duration getFlushInterval() {
      return flushInterval;
    }

    /** Sets {@code flush-interval}. */
    public void setFlushInterval(Duration flushInterval) {
      this.flushInterval = flushInterval;
    }

    /** Returns {@code max-bytes}. */
    public long getMaxBytes() {
      return maxBytes;
    }

    /** Sets {@code max-bytes}, as a number of bytes or a value such as {@code 5MB}. */
    public void setMaxBytes(@ReadableBytes long maxBytes) {
      this.maxBytes = maxBytes;
    }
  }
}
