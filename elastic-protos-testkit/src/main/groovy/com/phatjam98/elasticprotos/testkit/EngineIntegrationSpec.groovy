package com.phatjam98.elasticprotos.testkit

import static com.phatjam98.elasticprotos.proto.SearchOperationType.ANY_IN
import static com.phatjam98.elasticprotos.proto.SearchOperationType.EQUALS
import static com.phatjam98.elasticprotos.proto.SearchOperationType.EXISTS
import static com.phatjam98.elasticprotos.proto.SearchOperationType.GEO_DISTANCE
import static com.phatjam98.elasticprotos.proto.SearchOperationType.NUM_RANGE
import static com.phatjam98.elasticprotos.proto.SearchOperationType.TEXT_CONTAINS
import static com.phatjam98.elasticprotos.proto.SearchOperationType.TEXT_MATCHES
import static com.phatjam98.elasticprotos.proto.SearchOperationType.TEXT_PREFIX
import static com.phatjam98.elasticprotos.proto.SearchOperationType.TEXT_REGEX

import com.google.protobuf.Any
import com.google.protobuf.FieldMask
import com.google.protobuf.Message
import com.google.protobuf.StringValue
import com.google.protobuf.Struct
import com.google.protobuf.Timestamp
import com.google.protobuf.TypeRegistry
import com.google.protobuf.Value
import com.phatjam98.elasticprotos.codec.CodecConfig
import com.phatjam98.elasticprotos.codec.ProtoDocumentCodec
import com.phatjam98.elasticprotos.engine.BulkOp
import com.phatjam98.elasticprotos.engine.EngineException
import com.phatjam98.elasticprotos.engine.EngineProduct
import com.phatjam98.elasticprotos.engine.Hit
import com.phatjam98.elasticprotos.engine.IndexOptions
import com.phatjam98.elasticprotos.engine.ReindexRequest
import com.phatjam98.elasticprotos.engine.SearchEngine
import com.phatjam98.elasticprotos.engine.SearchResult
import com.phatjam98.elasticprotos.engine.UpdateByQueryRequest
import com.phatjam98.elasticprotos.mapping.IndexDefinition
import com.phatjam98.elasticprotos.mapping.IndexNamingStrategy
import com.phatjam98.elasticprotos.mapping.MappingOverrides
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.migration.Action
import com.phatjam98.elasticprotos.migration.DiffClassifier
import com.phatjam98.elasticprotos.migration.IndexMigrator
import com.phatjam98.elasticprotos.migration.IndexPlan
import com.phatjam98.elasticprotos.migration.MigrationListener
import com.phatjam98.elasticprotos.migration.MigrationListener.Phase
import com.phatjam98.elasticprotos.migration.MigrationOptions
import com.phatjam98.elasticprotos.migration.MigrationPlan
import com.phatjam98.elasticprotos.migration.MigrationResult
import com.phatjam98.elasticprotos.migration.MigrationScriptProvider
import com.phatjam98.elasticprotos.migration.ReconcileStats
import com.phatjam98.elasticprotos.migration.RollbackMode
import com.phatjam98.elasticprotos.migration.StaticSettingsPolicy
import com.phatjam98.elasticprotos.migration.RemovedFieldPolicy
import com.phatjam98.elasticprotos.proto.Bool3
import com.phatjam98.elasticprotos.proto.CalendarInterval
import com.phatjam98.elasticprotos.proto.DateBounds
import com.phatjam98.elasticprotos.proto.HistogramAggregation
import com.phatjam98.elasticprotos.proto.TermsAggregation
import com.phatjam98.elasticprotos.proto.TermsOrder
import com.phatjam98.elasticprotos.proto.TermsOrderBy
import com.phatjam98.elasticprotos.proto.DistanceUnit
import com.phatjam98.elasticprotos.proto.EsType
import com.phatjam98.elasticprotos.proto.FieldCondition
import com.phatjam98.elasticprotos.proto.FieldSort
import com.phatjam98.elasticprotos.proto.FlatValue
import com.phatjam98.elasticprotos.proto.GeoDistanceValue
import com.phatjam98.elasticprotos.proto.LatLon
import com.phatjam98.elasticprotos.proto.Pagination
import com.phatjam98.elasticprotos.proto.PrimitiveList
import com.phatjam98.elasticprotos.proto.PrimitiveValue
import com.phatjam98.elasticprotos.proto.RangeValue
import com.phatjam98.elasticprotos.proto.SearchCondition
import com.phatjam98.elasticprotos.proto.SearchCriteria
import com.phatjam98.elasticprotos.proto.SearchOperationType
import com.phatjam98.elasticprotos.proto.SortingCriteria
import com.phatjam98.elasticprotos.proto.SortingOrder
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.proto.example.InboxItem
import com.phatjam98.elasticprotos.proto.example.InnerMaps
import com.phatjam98.elasticprotos.proto.example.InnerPart
import com.phatjam98.elasticprotos.proto.example.InnerValue
import com.phatjam98.elasticprotos.proto.example.Office
import com.phatjam98.elasticprotos.proto.example.TypedBag
import com.phatjam98.elasticprotos.proto.example.TypedChild
import com.phatjam98.elasticprotos.proto.example.TypedItem
import com.phatjam98.elasticprotos.proto.example.TypedMapLevel
import com.phatjam98.elasticprotos.proto.example.TypedMaps
import com.phatjam98.elasticprotos.query.AggregationResults
import com.phatjam98.elasticprotos.query.Aggs
import com.phatjam98.elasticprotos.query.Criteria
import com.phatjam98.elasticprotos.query.QueryCompiler
import com.phatjam98.elasticprotos.query.QueryCompilerConfig
import com.phatjam98.elasticprotos.query.SearchPager
import java.time.Duration
import java.time.Instant
import org.testcontainers.DockerClientFactory
import spock.lang.Requires
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise

/**
 * The behaviour every {@link SearchEngine} adapter must show against a real server: bootstrap,
 * every query operation, nested same-element matching, sorting, round trips, realtime get by id,
 * point-in-time paging, additive and breaking migrations, and error classification.
 *
 * <p>Subclasses start a server (usually a Testcontainers container) in {@link #createEngine()} and
 * stop it in {@link #releaseEngine()}. Every index and alias the spec creates carries a random
 * {@code it-<random>} prefix and is deleted in {@code cleanupSpec}, so runs against a shared,
 * long-lived server do not collide. The spec is skipped when Docker is unavailable.
 */
@Stepwise
@Requires(value = { Boolean.getBoolean('elasticprotos.requireDocker') || DockerClientFactory.instance().isDockerAvailable() }, inherited = true,
    reason = 'Docker is not available')
abstract class EngineIntegrationSpec extends Specification {

  /** The script applied by the breaking migration: tags every copied document. */
  static final String MIGRATION_SCRIPT =
      "if (ctx._source.tags == null) { ctx._source.tags = []; } ctx._source.tags.add('migrated')"

  static final int PAGING_DOCS = 2500

  /** Migration options for the reindexing features: a short swap grace keeps them fast. */
  static final MigrationOptions FAST = MigrationOptions.standard().withSwapGrace(Duration.ofMillis(200))

  @Shared SearchEngine engine
  @Shared String prefix = 'it-' + UUID.randomUUID().toString().substring(0, 8)
  @Shared IndexNamingStrategy naming = IndexNamingStrategy.prefixed(prefix)
  @Shared ProtoMapper mapper = ProtoMapper.builder().naming(naming).build()
  @Shared ProtoDocumentCodec codec = ProtoDocumentCodec.of(mapper)
  @Shared QueryCompiler compiler = QueryCompiler.of(QueryCompilerConfig.standard().withAllowRegex(true))
  @Shared IndexMigrator migrator
  @Shared IndexDefinition firms = mapper.map(Firm)
  @Shared IndexDefinition documents = mapper.map(Document)

  // state carried between the ordered features
  @Shared String originalDocumentIndex
  @Shared IndexDefinition keywordDocuments
  @Shared IndexPlan reindexPlan

  /** Connects to the server under test; called once, before any feature. */
  abstract SearchEngine createEngine()

  /** The Docker image of the server under test, for example {@code opensearchproject/opensearch:3.9.0}. */
  abstract String engineImage()

  /** Stops the server; called after the spec has deleted its indices and closed the engine. */
  void releaseEngine() {}

  /** The product the server should report; derived from the image name by default. */
  EngineProduct expectedProduct() {
    engineImage().contains('opensearch') ? EngineProduct.OPENSEARCH : EngineProduct.ELASTICSEARCH
  }

  /** The version the server should report; the image tag by default. */
  String expectedVersion() {
    engineImage().substring(engineImage().lastIndexOf(':') + 1)
  }

  def setupSpec() {
    engine = createEngine()
    migrator = IndexMigrator.builder().engine(engine).mapper(mapper).build()
  }

  def cleanupSpec() {
    try {
      if (engine != null) {
        engine.getAliases(prefix + '-*').keySet().each { index ->
          try {
            engine.deleteIndex(index)
          } catch (EngineException ignored) {
            // best effort
          }
        }
        engine.close()
      }
    } finally {
      releaseEngine()
    }
  }

  // ------------------------------------------------------------------ cluster

  def "info reports the product and version, and health answers"() {
    when:
    def info = engine.info()
    def health = engine.health()

    then:
    info.product() == expectedProduct()
    info.version() == expectedVersion()
    info.supportsPointInTime()
    health.status() in ['green', 'yellow']
    health.numberOfNodes() >= 1
  }

  // ------------------------------------------------------------------ bootstrap

  def "bootstrap creates both indices with their aliases, and a second bootstrap does nothing"() {
    when:
    MigrationPlan plan = migrator.plan(Firm, Document)
    String report = plan.toReport()
    MigrationResult result = migrator.apply(plan)

    then:
    result.ok()
    plan.items()*.action() == [Action.CREATE, Action.CREATE]
    report.readLines().count { it.trim().startsWith('CREATE ') } == 2
    report.contains(firms.alias())
    report.contains(documents.alias())
    engine.getAliases(firms.alias()).keySet() == [firms.name()] as Set
    engine.getAliases(documents.alias()).keySet() == [documents.name()] as Set
    firms.alias() == prefix + '-firms'
    documents.name().startsWith(prefix + '-documents-v')

    when:
    MigrationResult again = migrator.bootstrap(Firm, Document)

    then:
    again.ok()
    again.outcomes()*.plan()*.action() == [Action.NO_OP, Action.NO_OP]

    and: 'the live mappings, once normalized, are what the mapper produced'
    [firms, documents].every { definition ->
      !DiffClassifier.diff(engine.getMappings(definition.alias()), definition.mappings(), [:], [:])
          .hasFieldChanges()
    }
    DiffClassifier.normalize(engine.getMappings(documents.alias())) ==
        DiffClassifier.normalize(documents.mappings())

    cleanup:
    originalDocumentIndex = documents.name()
  }

  // ------------------------------------------------------------------ indexing

  def "the sample set is bulk indexed through the codec"() {
    given:
    List<BulkOp> ops = []
    SampleData.firms().each { ops << BulkOp.index(firms.alias(), codec.idOf(it).get(), codec.toDocument(it)) }
    SampleData.documents().each { ops << BulkOp.index(documents.alias(), codec.idOf(it).get(), codec.toDocument(it)) }

    when:
    def result = engine.bulk(ops)
    engine.refresh(firms.alias())
    engine.refresh(documents.alias())

    then:
    !result.errors()
    result.failures().isEmpty()
    result.items().size() == 9
    count(firms.alias()) == 3
    count(documents.alias()) == 6

    and: 'map fields are flattened: their keys never reach the mapping, so the plan stays a no-op'
    engine.getMappings(firms.alias()).properties.labels.type == 'flattened'
    engine.getMappings(firms.alias()).properties.labels.properties == null
    migrator.plan(Firm).items().every { it.action() == Action.NO_OP && it.warnings().isEmpty() }
  }

  // ------------------------------------------------------------------ query operations

  def "EQUALS matches a keyword field exactly"() {
    expect:
    docIds(where(fc('firm_id', EQUALS, str('firm-2')))) == ['doc-2', 'doc-3'] as Set
    docIds(where(fc('type', EQUALS, str('NOTICE')))) == ['doc-4'] as Set
  }

  def "EQUALS on a text field uses its keyword sub-field"() {
    expect:
    docIds(where(fc('title', EQUALS, str('Motion to dismiss')))) == ['doc-2'] as Set
    docIds(where(fc('title', EQUALS, str('motion to dismiss')))).isEmpty()
  }

  def "EXISTS matches documents that carry the field"() {
    expect:
    docIds(where(fc('relevance', EXISTS))) == ['doc-6'] as Set
    docIds(where(fc('relevance', EXISTS, FlatValue.defaultInstance, true))).size() == 5
  }

  def "TEXT_MATCHES runs an analyzed match"() {
    expect:
    docIds(where(fc('body', TEXT_MATCHES, str('summary judgment')))) == ['doc-3'] as Set
    docIds(where(fc('body', TEXT_MATCHES, str('dismissed')))) == ['doc-2'] as Set // english stemming
  }

  def "TEXT_CONTAINS runs a phrase match"() {
    expect:
    docIds(where(fc('body', TEXT_CONTAINS, str('failure to state a claim')))) == ['doc-2'] as Set
    docIds(where(fc('body', TEXT_CONTAINS, str('claim to state failure')))).isEmpty()
  }

  def "TEXT_PREFIX matches the start of keyword values"() {
    expect:
    docIds(where(fc('tags', TEXT_PREFIX, str('civ')))) == ['doc-2', 'doc-3'] as Set
  }

  def "TEXT_REGEX matches keyword values"() {
    expect:
    docIds(where(fc('id', TEXT_REGEX, str('doc-[13]')))) == ['doc-1', 'doc-3'] as Set
  }

  def "NUM_RANGE filters a long field"() {
    expect:
    docIds(where(fc('size_bytes', NUM_RANGE, range(10_000, 100_000)))) == ['doc-2', 'doc-3'] as Set
    docIds(where(fc('size_bytes', NUM_RANGE, range(45_000, 120_000, false, true)))) == ['doc-1'] as Set
  }

  def "NUM_RANGE filters a date field by epoch millis"() {
    given:
    Instant from = Instant.parse('2024-02-01T00:00:00Z')
    Instant to = Instant.parse('2024-04-01T00:00:00Z')

    expect:
    docIds(Criteria.where('filed_at').between(from, to).build()) == ['doc-2', 'doc-3'] as Set
  }

  def "NUM_RANGE with only a lower bound matches at or above it"() {
    given:
    long threshold = 120_000L
    Set<String> expected = SampleData.documents().findAll { it.sizeBytes >= threshold }*.id as Set

    expect:
    expected == ['doc-1', 'doc-5', 'doc-6'] as Set
    docIds(Criteria.where('size_bytes').atLeast(threshold).build()) == expected
  }

  def "NUM_RANGE with an exclusive lower date bound matches strictly after it"() {
    given:
    Instant after = Instant.parse('2024-03-10T09:15:00Z')
    Set<String> expected = SampleData.documents()
        .findAll { Instant.ofEpochSecond(it.filedAt.seconds, it.filedAt.nanos).isAfter(after) }*.id as Set

    expect: 'the document filed exactly at the bound is excluded'
    expected == ['doc-4', 'doc-5', 'doc-6'] as Set
    docIds(Criteria.where('filed_at').after(after).build()) == expected
  }

  def "ANY_IN matches any of the listed values"() {
    expect:
    docIds(where(fc('tags', ANY_IN, list('urgent', 'admin')))) == ['doc-2', 'doc-4'] as Set
  }

  def "a negated predicate excludes its matches"() {
    expect:
    docIds(where(fc('type', EQUALS, str('MOTION'), true))) ==
        ['doc-1', 'doc-3', 'doc-4', 'doc-5', 'doc-6'] as Set
    docIds(where(fc('type', EQUALS, str('FILING')), fc('tags', EQUALS, str('discovery'), true))) ==
        ['doc-1', 'doc-5'] as Set
  }

  def "proto3 default values are indexed, so EQUALS and ranges find them"() {
    given: 'a firm whose status, domain and office headcount hold their defaults, with no timestamps or headquarters'
    Firm firm = Firm.newBuilder().setId('firm-defaults').setName('Default & Co')
        .addOffices(Office.newBuilder().setName('Annex')).build()
    Map<String, Object> doc = codec.toDocument(firm)
    engine.index(firms.alias(), 'firm-defaults', doc, IndexOptions.refreshed())

    expect: 'the document carries the defaults and leaves unset messages out'
    doc.status == 'FIRM_STATUS_UNSPECIFIED'
    doc.domain == ''
    doc.offices == [[headcount: 0, name: 'Annex']]
    !doc.containsKey('created_at')
    !doc.containsKey('headquarters')

    and: 'default values match like any other value'
    firmIds(where(fc('status', EQUALS, str('FIRM_STATUS_UNSPECIFIED')))) == ['firm-defaults'] as Set
    firmIds(where(fc('domain', EQUALS, str('')))) == ['firm-defaults'] as Set
    firmIds(where(fc('offices.headcount', EQUALS, integer(0)))) == ['firm-defaults'] as Set
    firmIds(where(fc('offices.headcount', NUM_RANGE, range(0, 5)))) == ['firm-defaults'] as Set
    firmIds(where(fc('status', EQUALS, str('ACTIVE')), fc('status', EXISTS))) == ['firm-1', 'firm-2'] as Set

    and: 'an unset timestamp and sub-message stay absent'
    firmIds(where(fc('created_at', EXISTS, FlatValue.defaultInstance, true))) == ['firm-defaults'] as Set
    firmIds(where(fc('headquarters.city', EXISTS, FlatValue.defaultInstance, true))) == ['firm-defaults'] as Set

    and: 'it reads back to the same message'
    codec.fromHit(engine.get(firms.alias(), 'firm-defaults').get(), Firm) == firm

    cleanup:
    engine.delete(firms.alias(), 'firm-defaults', IndexOptions.refreshed())
  }

  def "a key of a flattened field is queried by its dotted path"() {
    expect: 'map<string, string> labels'
    firmIds(where(fc('labels.tier', EQUALS, str('gold')))) == ['firm-1'] as Set
    firmIds(where(fc('labels.tier', EQUALS, str('silver')))).isEmpty()
    firmIds(where(fc('labels.tier', ANY_IN, list('silver', 'gold')))) == ['firm-1'] as Set
    firmIds(where(fc('labels.tier', EXISTS))) == ['firm-1'] as Set
    firmIds(where(fc('labels.tier', TEXT_PREFIX, str('go')))) == ['firm-1'] as Set
    firmIds(where(fc('labels.tier', EQUALS, str('gold'), true))) == ['firm-2', 'firm-3'] as Set

    and: 'a Struct mapped as flattened'
    docIds(where(fc('metadata.court_reporter', EQUALS, str('A. Smith')))) == ['doc-6'] as Set
  }

  def "predicates on one nested path must hold for the same element"() {
    given: 'Acme Corp is a plaintiff in doc-1, but a defendant in doc-2 where Jane Doe is the plaintiff'
    def criteria = where(
        fc('parties.name', TEXT_CONTAINS, str('Acme Corp')),
        fc('parties.role', EQUALS, str('PLAINTIFF')))
    def ungrouped = QueryCompiler.of(QueryCompilerConfig.standard().withGroupNested(false))

    expect:
    docIds(criteria) == ['doc-1'] as Set

    and: 'without grouping the cross-element match wrongly includes doc-2'
    ids(engine.search([documents.alias()], ungrouped.compile(criteria, documents).body())) ==
        ['doc-1', 'doc-2'] as Set
  }

  def "GEO_DISTANCE finds firms near a point"() {
    given:
    def vegas = SampleData.LAS_VEGAS

    expect:
    firmIds(Criteria.where('headquarters.location').withinDistance(vegas.lat, vegas.lon, '50km').build()) ==
        ['firm-1'] as Set
    firmIds(Criteria.where('headquarters.location').withinDistance(vegas.lat, vegas.lon, '1000km').build()) ==
        ['firm-1', 'firm-3'] as Set
  }

  def "GEO_BOUNDING_BOX finds firms inside a box"() {
    expect: 'a box over the American southwest'
    firmIds(Criteria.where('headquarters.location').inBoundingBox(38, -120, 34, -104).build()) ==
        ['firm-1', 'firm-3'] as Set
  }

  // ------------------------------------------------------------------ full text

  def "FULL_TEXT ranks the document with both words first and leaves out documents with neither"() {
    when:
    def compiled = compiler.compile(Criteria.builder().fullText('merger agreement').build(), documents)
    def result = engine.search([documents.alias()], compiled.body())

    then:
    compiled.warnings().isEmpty()
    result.hits()[0].id() == 'doc-1'
    result.hits()*.id().contains('doc-5')
    !result.hits()*.id().contains('doc-6')
    result.hits()[0].score() > result.hits().find { it.id() == 'doc-5' }.score()
  }

  // ------------------------------------------------------------------ sorting

  def "sorting by a date field descending"() {
    expect:
    orderedIds(documents, sorted(fieldSort('filed_at', SortingOrder.DESC))) ==
        ['doc-6', 'doc-5', 'doc-4', 'doc-3', 'doc-2', 'doc-1']
  }

  def "sorting by a text field uses its keyword sub-field"() {
    expect:
    orderedIds(documents, sorted(fieldSort('title', SortingOrder.ASC))) ==
        ['doc-5', 'doc-6', 'doc-1', 'doc-2', 'doc-4', 'doc-3']
  }

  def "sorting by distance from a point"() {
    given:
    def criteria = Criteria.builder().sortByDistance('headquarters.location', SampleData.LAS_VEGAS.lat,
        SampleData.LAS_VEGAS.lon, DistanceUnit.KILOMETERS, SortingOrder.ASC).build()

    expect:
    orderedIds(firms, criteria) == ['firm-1', 'firm-3', 'firm-2']
  }

  // ------------------------------------------------------------------ aggregations

  def "terms aggregations bucket keyword and text fields, with metric sub-aggregations"() {
    when:
    def aggs = aggregate(documents, Criteria.builder().aggregate(
        Aggs.sub(Aggs.terms('by_type', 'type'), Aggs.sum('total', 'size_bytes'))))

    then: 'by count, ties by key'
    aggs.buckets('by_type')*.key() == ['FILING', 'MOTION', 'NOTICE', 'ORDER']
    aggs.buckets('by_type')*.docCount() == [3L, 1L, 1L, 1L]
    aggs.buckets('by_type')*.sub()*.metric('total')*.asLong() == [2_870_000L, 45_000L, 8_000L, 30_000L]
    aggs.sumOtherDocCount('by_type') == 0

    when: 'firms by status, and by name through its keyword sub-field'
    def firmAggs = aggregate(firms, Criteria.builder().aggregate(
        Aggs.terms('status', 'status'), Aggs.terms('names', 'name')))

    then:
    firmAggs.buckets('status').collectEntries { [(it.key()): it.docCount()] } == [ACTIVE: 2L, SUSPENDED: 1L]
    firmAggs.buckets('names')*.key() ==
        ['Dewey Cheatem & Howe LLP', 'Hamlin Hamlin & McGill', 'Pearson Specter Litt']
  }

  def "metric aggregations over a long, a keyword and a date field"() {
    when:
    def aggs = aggregate(documents, Criteria.builder().aggregate(
        Aggs.min('smallest', 'size_bytes'), Aggs.max('largest', 'size_bytes'),
        Aggs.avg('average', 'size_bytes'), Aggs.sum('total', 'size_bytes'),
        Aggs.count('counted', 'size_bytes'), Aggs.cardinality('firms', 'firm_id'),
        Aggs.max('latest', 'filed_at'), Aggs.min('lowest_relevance', 'relevance')))

    then:
    aggs.metric('smallest').asLong() == 8_000L
    aggs.metric('largest').asLong() == 2_500_000L
    aggs.metric('total').asLong() == 2_953_000L
    Math.abs(aggs.metric('average').value() - 2_953_000d / 6) < 0.01
    aggs.metric('counted').asLong() == 6L
    aggs.metric('firms').asLong() == 3L
    aggs.metric('latest').valueAsString().startsWith('2024-06-30')
    aggs.metric('latest').asLong() == SampleData.ts('2024-06-30T08:00:00Z').seconds * 1000
    aggs.metric('lowest_relevance').value() == 0.75d
  }

  def "date histograms bucket by calendar month, with empty edge months when bounds are forced"() {
    when:
    def aggs = aggregate(documents, Criteria.builder().aggregate(
        Aggs.dateHistogram('per_month', 'filed_at', CalendarInterval.MONTH)
            .tap { dateHistogramBuilder.setFormat('yyyy-MM') },
        Aggs.dateHistogram('padded', 'filed_at', CalendarInterval.MONTH).tap {
          dateHistogramBuilder.setFormat('yyyy-MM').setMinDocCount(0).setExtendedBounds(DateBounds.newBuilder()
              .setMin(SampleData.ts('2023-12-01T00:00:00Z')).setMax(SampleData.ts('2024-07-31T00:00:00Z')))
        }))

    then:
    aggs.buckets('per_month')*.keyAsString() == ['2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06']
    aggs.buckets('per_month')*.docCount() == [1L] * 6
    aggs.buckets('per_month')[0].key() == SampleData.ts('2024-01-01T00:00:00Z').seconds * 1000

    and:
    aggs.buckets('padded')*.keyAsString() ==
        ['2023-12', '2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06', '2024-07']
    aggs.buckets('padded')*.docCount() == [0L, 1L, 1L, 1L, 1L, 1L, 1L, 0L]
  }

  def "histogram, range and date range aggregations"() {
    when:
    def aggs = aggregate(documents, Criteria.builder().aggregate(
        Aggs.histogram('sizes', 'size_bytes', 100_000).setHistogram(
            HistogramAggregation.newBuilder().setInterval(100_000).setMinDocCount(1)),
        Aggs.range('bands', 'size_bytes', Aggs.bucket('small', null, 50_000d),
            Aggs.bucket('medium', 50_000d, 500_000d), Aggs.bucket('large', 500_000d, null)),
        Aggs.range('quarters', 'filed_at', Aggs.bucket('q1_2024',
            Instant.parse('2024-01-01T00:00:00Z'), Instant.parse('2024-04-01T00:00:00Z')),
            Aggs.bucket('later', SampleData.ts('2024-04-01T00:00:00Z').seconds * 1000d, (Double) null))))

    then:
    aggs.buckets('sizes')*.key() == [0d, 100_000d, 200_000d, 2_500_000d]
    aggs.buckets('sizes')*.docCount() == [3L, 1L, 1L, 1L]

    and: 'range buckets are [from, to)'
    aggs.buckets('bands')*.key() == ['small', 'medium', 'large']
    aggs.buckets('bands')*.docCount() == [3L, 2L, 1L]
    aggs.buckets('bands')[1].from() == 50_000d
    aggs.buckets('bands')[1].to() == 500_000d
    aggs.buckets('bands')[0].from() == null

    and:
    aggs.buckets('quarters')*.key() == ['q1_2024', 'later']
    aggs.buckets('quarters')*.docCount() == [3L, 3L]
    aggs.buckets('quarters')[0].docCount() == 3L
    aggs.buckets('quarters')[0].from() == SampleData.ts('2024-01-01T00:00:00Z').seconds * 1000d
  }

  def "nested terms with a reverse-nested metric, ordered by that metric, and aggregations only"() {
    when:
    def compiled = compiler.compile(Criteria.builder().aggregate(Aggs.sub(
        Aggs.terms('roles', 'parties.role').setTerms(TermsAggregation.newBuilder()
            .addOrder(TermsOrder.newBuilder().setBy(TermsOrderBy.BY_METRIC).setMetric('firms'))),
        Aggs.cardinality('firms', 'firm_id'))).aggregationsOnly().build(), documents)
    def result = engine.search([documents.alias()], compiled.body())
    def aggs = AggregationResults.of(result)

    then: 'no hits, but the total'
    result.hits().isEmpty()
    result.total() == 6

    and: 'parties per role, ordered by the number of distinct firms of their documents'
    aggs.names() == ['roles'] as Set
    aggs.buckets('roles')*.key() == ['DEFENDANT', 'PLAINTIFF', 'COUNSEL']
    aggs.buckets('roles')*.docCount() == [4L, 3L, 1L]
    aggs.buckets('roles')*.sub()*.metric('firms')*.asLong() == [3L, 2L, 1L]
  }

  def "aggregations cover only the documents the query matches"() {
    when:
    def aggs = aggregate(documents, Criteria.where('type').equalTo('FILING').end()
        .aggregate(Aggs.terms('tags', 'tags'), Aggs.count('docs', 'size_bytes')))

    then:
    aggs.buckets('tags').collectEntries { [(it.key()): it.docCount()] } ==
        [corporate: 2L, discovery: 1L, 'm-and-a': 1L]
    aggs.metric('docs').asLong() == 3L
  }

  def "terms on a key of a flattened field return buckets on every engine, flat_object on OpenSearch included"() {
    when: 'verified on Elasticsearch 8.19 and 9.5 and OpenSearch 2.19 and 3.9'
    def aggs = aggregate(firms, Criteria.builder().aggregate(Aggs.terms('tiers', 'labels.tier'),
        Aggs.cardinality('tier_count', 'labels.tier')))

    then:
    aggs.buckets('tiers').collectEntries { [(it.key()): it.docCount()] } == [gold: 1L]
    aggs.metric('tier_count').asLong() == 1L
  }

  def "highlighting marks the matched terms of text fields"() {
    when:
    def compiled = compiler.compile(Criteria.builder().fullText('merger agreement').highlight().build(), documents)
    def hits = engine.search([documents.alias()], compiled.body()).hits()

    then:
    compiled.warnings().isEmpty()
    hits[0].id() == 'doc-1'
    hits[0].highlight()['title'].any { it.contains('<em>Merger</em>') }
    hits[0].highlight()['body'].any { it.contains('<em>merger</em>') }

    when: 'custom tags and HTML encoding'
    def custom = compiler.compile(Criteria.where('name').matches('Dewey').end().highlight {
      it.addFields(com.phatjam98.elasticprotos.proto.HighlightField.newBuilder().setField('name'))
          .addPreTags('[').addPostTags(']').setHtmlEncode(true)
    }.build(), firms)
    def firmHits = engine.search([firms.alias()], custom.body()).hits()

    then:
    firmHits*.id() == ['firm-1']
    firmHits[0].highlight()['name'] == ['[Dewey] Cheatem &amp; Howe LLP']
  }

  // ------------------------------------------------------------------ typed maps

  def "typed maps: every key takes the value type through dynamic templates"() {
    given: 'its own index under a separate prefix, so the shared sample indices stay untouched'
    def tmMapper = ProtoMapper.builder().naming(IndexNamingStrategy.prefixed(prefix + '-tm')).build()
    def tmCodec = ProtoDocumentCodec.of(tmMapper)
    def tmMigrator = IndexMigrator.builder().engine(engine).mapper(tmMapper).build()
    IndexDefinition typed = tmMapper.map(TypedMaps)
    def typedIds = { SearchCriteria criteria -> docIds(criteria, typed) }
    def due = Timestamp.newBuilder().setSeconds(1_709_528_767L).build()
    def first = TypedMaps.newBuilder()
        .setId('tm-1')
        .putCounters('views', 5L)
        .putNotes('summary', 'Quarterly reports are late')
        .putDue('review', due)
        .putLevels('east', TypedMapLevel.HIGH)
        .putPlaces('home', LatLon.newBuilder().setLat(36.17d).setLon(-115.14d).build())
        .putChildren('alice', TypedChild.newBuilder().setName('Alice').setCount(3L).setDisplayName('Ally')
            .addItems(TypedItem.newBuilder().setSku('A-1').setQuantity(2))
            .addItems(TypedItem.newBuilder().setSku('B-2').setQuantity(9)).build())
        .putBags('b', TypedBag.newBuilder().putTags('k', 'v').build())
        .putLabels('env', 'prod')
        .putAttributes('a', Struct.newBuilder().putFields('x', Value.newBuilder().setStringValue('y').build()).build())
        .build()
    def second = TypedMaps.newBuilder()
        .setId('tm-2')
        .putCounters('views', 15L)
        .putCounters('clicks', 2L)
        .putNotes('other', 'nothing to see here')
        .build()

    when: 'bootstrapped'
    def created = tmMigrator.bootstrap(TypedMaps)
    def stored = engine.getMappings(typed.alias())

    then: 'the engine stores the templates as sent, so a re-plan is a no-op'
    created.ok()
    typed.alias() == prefix + '-tm-typed_maps'
    DiffClassifier.normalize(stored).dynamic_templates ==
        DiffClassifier.normalize(typed.mappings()).dynamic_templates
    tmMigrator.plan(TypedMaps).items()*.action() == [Action.NO_OP]

    when: 'two documents are indexed'
    def bulk = engine.bulk([first, second].collect {
      BulkOp.index(typed.alias(), tmCodec.idOf(it).get(), tmCodec.toDocument(it))
    })
    engine.refresh(typed.alias())
    def live = engine.getMappings(typed.alias()).properties

    then: 'each key was mapped by its template, and the plan stays a no-op'
    !bulk.errors()
    live.counters.properties.views.type == 'long'
    live.counters.properties.clicks.type == 'long'
    live.due.properties.review.type == 'date'
    live.notes.properties.summary.type == 'text'
    live.notes.properties.summary.analyzer == 'english'
    live.notes.properties.summary.fields.keyword.type == 'keyword'
    live.levels.properties.east.type == 'keyword'
    live.places.properties.home.type == 'geo_point'
    live.children.properties.alice.properties.count.type == 'long'
    live.children.properties.alice.properties.label.type == 'keyword'
    live.children.properties.alice.properties.items.type == 'nested'
    live.children.properties.alice.properties.items.properties.quantity.type == 'integer'
    tmMigrator.plan(TypedMaps).items()*.action() == [Action.NO_OP]

    and: 'keys are queried with the operations of their value type'
    typedIds(where(fc('counters.views', NUM_RANGE, range(10, 100)))) == ['tm-2'] as Set
    typedIds(where(fc('counters.views', EQUALS, integer(5)))) == ['tm-1'] as Set
    typedIds(where(fc('counters.clicks', EXISTS))) == ['tm-2'] as Set
    typedIds(where(fc('notes.summary', TEXT_MATCHES, str('report')))) == ['tm-1'] as Set
    typedIds(where(fc('notes.summary', EQUALS, str('Quarterly reports are late')))) == ['tm-1'] as Set
    typedIds(where(fullTextOn('notes.summary', 'reporting'))) == ['tm-1'] as Set
    typedIds(Criteria.builder().fullText('quarterly').build()) == ['tm-1'] as Set
    typedIds(where(fc('due.review', NUM_RANGE, range(1.7e12, 1.8e12)))) == ['tm-1'] as Set
    typedIds(where(fc('levels.east', ANY_IN, list('HIGH', 'LOW')))) == ['tm-1'] as Set
    typedIds(where(fc('places.home', GEO_DISTANCE, FlatValue.newBuilder().setGeoDistanceValue(
        GeoDistanceValue.newBuilder().setPoint(SampleData.LAS_VEGAS).setDistance('10km')).build()))) == ['tm-1'] as Set
    typedIds(where(fc('children.alice.label', EQUALS, str('Ally')))) == ['tm-1'] as Set

    and: 'a geo query on a key no document has used matches nothing instead of failing'
    typedIds(where(fc('places.nowhere', GEO_DISTANCE, FlatValue.newBuilder().setGeoDistanceValue(
        GeoDistanceValue.newBuilder().setPoint(SampleData.LAS_VEGAS).setDistance('10km')).build()))).isEmpty()

    and: 'nested values match per element, and a key no document has used matches nothing'
    typedIds(where(fc('children.alice.items.sku', EQUALS, str('A-1')),
        fc('children.alice.items.quantity', NUM_RANGE, range(1, 5)))) == ['tm-1'] as Set
    typedIds(where(fc('children.alice.items.sku', EQUALS, str('A-1')),
        fc('children.alice.items.quantity', NUM_RANGE, range(5, 10)))).isEmpty()
    typedIds(where(fc('children.bob.items.sku', EQUALS, str('A-1')))).isEmpty()

    and: 'sorting by a key, including one no document has'
    orderedIds(typed, sorted(fieldSort('counters.views', SortingOrder.DESC))) == ['tm-2', 'tm-1']
    orderedIds(typed, sorted(fieldSort('counters.nobody', SortingOrder.ASC),
        fieldSort('counters.views', SortingOrder.ASC))) == ['tm-1', 'tm-2']
    orderedIds(typed, sorted(fieldSort('notes.nobody', SortingOrder.ASC),
        fieldSort('counters.views', SortingOrder.DESC))) == ['tm-2', 'tm-1']

    and: 'hits parse back into the messages'
    engine.search([typed.alias()], compiler.compile(sorted(fieldSort('counters.views', SortingOrder.ASC)), typed).body())
        .hits().collect { tmCodec.fromHit(it, TypedMaps) } == [first, second]
  }

  def "typed maps inside typed map values: inner maps are flattened or keyed twice, Struct flattened, Any kept"() {
    given: 'its own index; outer key "metrics" and inner keys "score" and "parts" equal field names on purpose'
    def imMapper = ProtoMapper.builder().naming(IndexNamingStrategy.prefixed(prefix + '-im')).build()
    def imCodec = ProtoDocumentCodec.of(imMapper, CodecConfig.standard()
        .withAnyTypes(TypeRegistry.newBuilder().add(StringValue.descriptor).build()))
    def imMigrator = IndexMigrator.builder().engine(engine).mapper(imMapper).build()
    IndexDefinition im = imMapper.map(InnerMaps)
    def imIds = { SearchCriteria criteria -> docIds(criteria, im) }
    def at = Timestamp.newBuilder().setSeconds(1_709_528_767L).build()
    def part = { long qty, long count ->
      InnerPart.newBuilder().setQty(qty).setAt(at).setWhere(SampleData.LAS_VEGAS).putCounts('c', count).build()
    }
    def first = InnerMaps.newBuilder()
        .setId('im-1')
        .putOuter('metrics', InnerValue.newBuilder()
            .setScore(1d)
            .putLabels('env', 'prod')
            .putMetrics('score', 5d)
            .putMetrics('parts', 1d)
            .putParts('p', part(2L, 7L))
            .setExtra(Struct.newBuilder().putFields('x', Value.newBuilder().setStringValue('y').build()))
            .setPayload(Any.pack(StringValue.of('packed')))
            .setMask(FieldMask.newBuilder().addPaths('a.b').addPaths('c'))
            .build())
        .putNestedOuter('n', part(3L, 4L))
        .build()
    def second = InnerMaps.newBuilder()
        .setId('im-2')
        .putOuter('metrics', InnerValue.newBuilder().setScore(2d).putLabels('env', 'dev').putMetrics('score', 50d).build())
        .putOuter('k', InnerValue.newBuilder().putParts('score', part(9L, 1L)).build())
        .putNestedOuter('n', part(1L, 9L))
        .build()

    when: 'bootstrapped'
    def created = imMigrator.bootstrap(InnerMaps)
    def stored = engine.getMappings(im.alias())

    then: 'the engine stores the templates with two key wildcards as sent, so a re-plan is a no-op'
    created.ok()
    DiffClassifier.normalize(stored).dynamic_templates == DiffClassifier.normalize(im.mappings()).dynamic_templates
    imMigrator.plan(InnerMaps).items()*.action() == [Action.NO_OP]

    when: 'two documents are indexed'
    def bulk = engine.bulk([first, second].collect {
      BulkOp.index(im.alias(), imCodec.idOf(it).get(), imCodec.toDocument(it))
    })
    engine.refresh(im.alias())
    def live = engine.getMappings(im.alias()).properties
    def metrics = live.outer.properties.metrics.properties

    then: 'every path took the template it was generated for, even where a key equals a field name'
    !bulk.errors()
    metrics.score.type == 'double'
    metrics.labels.type == 'flattened'
    metrics.extra.type == 'flattened'
    metrics.metrics.properties.score.type == 'double'
    metrics.metrics.properties.parts.type == 'double'
    metrics.parts.properties.p.properties.qty.type == 'long'
    metrics.parts.properties.p.properties.at.type == 'date'
    metrics.parts.properties.p.properties.where.type == 'geo_point'
    metrics.parts.properties.p.properties.counts.properties.c.type == 'long'
    String.valueOf(metrics.payload.enabled) == 'false'
    String.valueOf(metrics.mask.enabled) == 'false'
    !metrics.payload.containsKey('properties')
    live.outer.properties.k.properties.parts.properties.score.properties.qty.type == 'long'
    live.nested_outer.properties.n.type == 'nested'
    live.nested_outer.properties.n.properties.counts.properties.c.type == 'long'
    imMigrator.plan(InnerMaps).items()*.action() == [Action.NO_OP]

    and: 'doubly keyed paths are queried by the inner value type'
    imIds(where(fc('outer.metrics.metrics.score', NUM_RANGE, range(10, 100)))) == ['im-2'] as Set
    imIds(where(fc('outer.metrics.metrics.score', NUM_RANGE, range(1, 10)))) == ['im-1'] as Set
    imIds(where(fc('outer.metrics.score', NUM_RANGE, range(1.5, 3)))) == ['im-2'] as Set
    imIds(where(fc('outer.metrics.parts.p.counts.c', NUM_RANGE, range(5, 10)))) == ['im-1'] as Set
    imIds(where(fc('outer.k.parts.score.qty', EQUALS, integer(9)))) == ['im-2'] as Set
    imIds(where(fc('outer.metrics.parts.p.where', GEO_DISTANCE, FlatValue.newBuilder().setGeoDistanceValue(
        GeoDistanceValue.newBuilder().setPoint(SampleData.LAS_VEGAS).setDistance('10km')).build()))) == ['im-1'] as Set
    imIds(where(fc('outer.nobody.metrics.score', NUM_RANGE, range(0, 100)))).isEmpty()

    and: 'a flattened map and a Struct inside the values answer EQUALS on a key'
    imIds(where(fc('outer.metrics.labels.env', EQUALS, str('prod')))) == ['im-1'] as Set
    imIds(where(fc('outer.metrics.extra.x', EQUALS, str('y')))) == ['im-1'] as Set

    and: 'inside nested values, predicates hold for the same element'
    imIds(where(fc('nested_outer.n.counts.c', EQUALS, integer(4)), fc('nested_outer.n.qty', NUM_RANGE, range(2, 5)))) ==
        ['im-1'] as Set
    imIds(where(fc('nested_outer.n.counts.c', EQUALS, integer(9)), fc('nested_outer.n.qty', NUM_RANGE, range(2, 5)))).isEmpty()

    and: 'sorting by a doubly keyed path, and hits parse back into the messages, Any and FieldMask included'
    orderedIds(im, sorted(fieldSort('outer.metrics.metrics.score', SortingOrder.DESC))) == ['im-2', 'im-1']
    engine.search([im.alias()], compiler.compile(sorted(fieldSort('outer.metrics.metrics.score', SortingOrder.ASC)), im).body())
        .hits().collect { imCodec.fromHit(it, InnerMaps) } == [first, second]
  }

  // ------------------------------------------------------------------ round trip

  def "hits parse back into the original messages, without ignored fields"() {
    when:
    def hits = engine.search([documents.alias()], compiler.compile(SearchCriteria.defaultInstance, documents).body()).hits()
    Map<String, Document> parsed = hits.collectEntries { [(it.id()): codec.fromHit(it, Document)] }
    Map<String, Document> expected = SampleData.documents().collectEntries {
      [(it.id): it.toBuilder().clearInternalNotes().build()]
    }

    then:
    parsed.keySet() == expected.keySet()
    parsed == expected
    parsed['doc-6'].metadata.fieldsMap['pages'].numberValue == 120d
    parsed['doc-6'].internalNotes.isEmpty()

    when:
    def firmHits = engine.search([firms.alias()], compiler.compile(SearchCriteria.defaultInstance, firms).body()).hits()

    then:
    firmHits.collectEntries { [(it.id()): codec.fromHit(it, Firm)] } ==
        SampleData.firms().collectEntries { [(it.id): it] }
  }

  // ------------------------------------------------------------------ realtime get

  def "get by id is realtime: it sees an unrefreshed write and a delete at once"() {
    given:
    Firm firm = SampleData.firms()[0].toBuilder().setId('firm-realtime').setName('Realtime LLP').build()

    when: 'written without refresh'
    engine.index(firms.alias(), 'firm-realtime', codec.toDocument(firm), IndexOptions.defaults())
    def hit = engine.get(firms.alias(), 'firm-realtime')

    then:
    hit.present
    hit.get().index() == firms.name()
    hit.get().id() == 'firm-realtime'
    hit.get().score() == null
    codec.fromHit(hit.get(), Firm) == firm
    !engine.get(firms.alias(), 'firm-missing').present
    !engine.get(prefix + '-missing', 'firm-realtime').present

    when: 'deleted with refresh'
    boolean existed = engine.delete(firms.alias(), 'firm-realtime', IndexOptions.refreshed())

    then:
    existed
    !engine.get(firms.alias(), 'firm-realtime').present
    count(firms.alias()) == 3
  }

  def "ids with reserved and non-ASCII characters round-trip through index, get and delete"() {
    given:
    String id = 'odd/id with space?and#hash'
    Firm firm = SampleData.firms()[0].toBuilder().setId(id).setName('Odd LLP').build()

    when:
    engine.index(firms.alias(), id, codec.toDocument(firm), IndexOptions.refreshed())
    def hit = engine.get(firms.alias(), id)

    then:
    hit.present
    hit.get().id() == id
    codec.fromHit(hit.get(), Firm) == firm

    when:
    boolean existed = engine.delete(firms.alias(), id, IndexOptions.refreshed())

    then:
    existed
    !engine.get(firms.alias(), id).present
    count(firms.alias()) == 3
  }

  // ------------------------------------------------------------------ paging

  def "SearchPager visits every document exactly once over a point in time"() {
    given:
    IndexDefinition inbox = mapper.map(InboxItem)
    assert migrator.bootstrap(InboxItem).ok()
    SampleData.inboxItems(PAGING_DOCS).collate(500).each { batch ->
      def bulk = engine.bulk(batch.collect { BulkOp.index(inbox.alias(), codec.idOf(it).get(), codec.toDocument(it)) })
      assert !bulk.errors()
    }
    engine.refresh(inbox.alias())
    def criteria = sorted(fieldSort('id', SortingOrder.ASC))
    List<String> seen = []
    int pages = 0

    when:
    SearchPager pager = SearchPager.open(engine, compiler, [inbox.alias()], criteria, inbox, 200)
    try {
      while (pager.hasNext()) {
        SearchResult page = pager.next()
        pages++
        seen.addAll(page.hits()*.id())
      }
    } finally {
      pager.close()
    }

    then:
    pages == 13
    seen.size() == PAGING_DOCS
    seen.toSet().size() == PAGING_DOCS
    seen == SampleData.inboxItems(PAGING_DOCS)*.id

    when: 'the closed point in time is used again'
    def stale = paged(Pagination.newBuilder().setSize(10).setCursor(pager.pagination().cursor).build(), criteria)
    engine.search([inbox.alias()], compiler.compile(stale, inbox).body())

    then:
    thrown(EngineException)
  }

  def "SearchPager pages through a keyword sort that some documents lack, which the engine returns as null"() {
    given: 'inbox items of which only every third carries document.firm_id'
    ProtoMapper sparseMapper = ProtoMapper.builder().naming(IndexNamingStrategy.prefixed(prefix + '-sparse')).build()
    IndexDefinition inbox = sparseMapper.map(InboxItem)
    assert IndexMigrator.builder().engine(engine).mapper(sparseMapper).build().bootstrap(InboxItem).ok()
    List<InboxItem> items = SampleData.inboxItems(25).withIndex().collect { InboxItem item, int i ->
      i % 3 == 0
          ? item.toBuilder().setDocument(Document.newBuilder().setFirmId('firm-' + (9 - i % 9))).build()
          : item
    }
    assert !engine.bulk(items.collect { BulkOp.index(inbox.alias(), codec.idOf(it).get(), codec.toDocument(it)) }).errors()
    engine.refresh(inbox.alias())
    def criteria = sorted(fieldSort('document.firm_id', SortingOrder.ASC), fieldSort('id', SortingOrder.ASC))
    List<Hit> seen = []

    when:
    SearchPager pager = SearchPager.open(engine, compiler, [inbox.alias()], criteria, inbox, 4)
    try {
      while (pager.hasNext()) {
        seen.addAll(pager.next().hits())
      }
    } finally {
      pager.close()
    }
    List<Hit> missing = seen.findAll { !(it.source().document instanceof Map) }
    println "${engineImage()}: sort values of a document without document.firm_id: ${missing[0].sort()}"

    then: 'every document is seen once, those with the field first in order, the rest after them'
    seen*.id().toSet().size() == 25
    seen.size() == 25
    seen.take(9)*.id() == items.findAll { it.hasDocument() }
        .sort { a, b -> a.document.firmId <=> b.document.firmId ?: a.id <=> b.id }*.id
    seen.drop(9)*.id() == items.findAll { !it.hasDocument() }*.id.sort()

    and: 'the missing sort value comes back as null'
    missing.size() == 16
    missing.every { it.sort()[0] == null }
  }

  // ------------------------------------------------------------------ migrations

  def "an added top-level field is applied in place, and reverting it reindexes into the next generation"() {
    given: 'a second documents alias, created by a mapper that leaves relevance out, with the sample set'
    IndexNamingStrategy putNaming = IndexNamingStrategy.prefixed(prefix + '-put')
    def without = ProtoMapper.builder().naming(putNaming)
        .overrides(MappingOverrides.builder().ignorePath('relevance').build()).build()
    def withRelevance = ProtoMapper.builder().naming(putNaming).build()
    IndexDefinition v1 = without.map(Document)
    assert IndexMigrator.builder().engine(engine).mapper(without).build().bootstrap(Document).ok()
    def v1Codec = ProtoDocumentCodec.of(without)
    def bulk = engine.bulk(SampleData.documents().collect {
      BulkOp.index(v1.alias(), v1Codec.idOf(it).get(), v1Codec.toDocument(it))
    })
    assert !bulk.errors()
    engine.refresh(v1.alias())
    def additive = IndexMigrator.builder().engine(engine).mapper(withRelevance).build()

    when:
    MigrationPlan plan = additive.plan(Document)
    def result = additive.apply(plan)

    then: 'the index keeps its name while its recorded hash moves on'
    plan.items()*.action() == [Action.PUT_MAPPING]
    plan.items()[0].diff().added()*.path() == ['relevance']
    plan.items()[0].diff().unindexedAdditions().isEmpty()
    result.ok()
    result.outcomes()[0].index() == v1.name()
    engine.getAliases(v1.alias()).keySet() == [v1.name()] as Set
    engine.getMappings(v1.alias())._meta.elastic_protos.hash == withRelevance.map(Document).contentHash()
    engine.getMappings(v1.alias()).properties.relevance.type == 'double'
    additive.plan(Document).items()*.action() == [Action.NO_OP]

    when: 'the proto goes back to v1, with removed fields reindexed'
    def revert = IndexMigrator.builder().engine(engine).mapper(without)
        .options(FAST.withRemovedFields(RemovedFieldPolicy.REINDEX)).build()
    MigrationPlan back = revert.plan(Document)
    def reverted = revert.apply(back)

    then: 'the live index already has the v1 name, so the new index is its second generation'
    back.items()*.action() == [Action.REINDEX]
    back.items()[0].target().name() == v1.name() + '-g2'
    reverted.ok()
    reverted.outcomes()[0].retainedIndices() == [v1.name()]
    engine.getAliases(v1.alias()).keySet() == [v1.name() + '-g2'] as Set
    count(v1.alias()) == 6
    engine.getMappings(v1.alias())._meta.elastic_protos.hash == v1.contentHash()
    revert.plan(Document).items()*.action() == [Action.NO_OP]
  }

  def "an added keyword multi-field is backfilled in place, so old documents are found through it"() {
    given:
    def overrides = MappingOverrides.builder()
        .forField(Document.descriptor, 'body') { it.setKeywordSubfield(Bool3.TRUE) }
        .build()
    def multiFieldMapper = ProtoMapper.builder().naming(naming).overrides(overrides).build()
    def multiField = IndexMigrator.builder().engine(engine).mapper(multiFieldMapper).options(FAST).build()
    keywordDocuments = multiFieldMapper.map(Document)

    when:
    MigrationPlan plan = multiField.plan(Document)

    then:
    plan.items()*.action() == [Action.BACKFILL]
    plan.items()[0].diff().added()*.path() == ['body.keyword']
    plan.items()[0].diff().unindexedAdditions()*.path() == ['body.keyword']
    plan.items()[0].currentIndex() == originalDocumentIndex

    when:
    def result = multiField.apply(plan)
    def live = engine.getMappings(documents.alias())

    then: 'the index keeps its name; the marker the backfill put into _meta is gone again'
    result.ok()
    result.outcomes()[0].index() == originalDocumentIndex
    result.outcomes()[0].retainedIndices().isEmpty()
    engine.getAliases(documents.alias()).keySet() == [originalDocumentIndex] as Set
    live._meta.elastic_protos.hash == keywordDocuments.contentHash()
    live._meta.elastic_protos.keySet() == ['hash', 'proto', 'schema_version', 'static_settings'] as Set
    live.properties.body.fields.keyword.type == 'keyword'
    multiField.plan(Document).items()*.action() == [Action.NO_OP]

    and: 'every document indexed before the migration is in the new sub-field'
    ids(engine.search([documents.alias()], [query: [exists: [field: 'body.keyword']], size: 20])) ==
        SampleData.documents()*.id as Set
    ids(engine.search([documents.alias()], [query: [term: ['body.keyword':
        'Counsel enters an appearance on behalf of the defendant.']]])) == ['doc-4'] as Set
    docIds(where(fc('body', EQUALS, str('Seller transfers the listed assets to the buyer at closing.'))),
        keywordDocuments) == ['doc-5'] as Set

    and: 'sorting by body uses the sub-field and orders the old documents'
    orderedIds(keywordDocuments, sorted(fieldSort('body', SortingOrder.ASC))) ==
        SampleData.documents().toSorted { it.body }*.id
  }

  def "with backfill off, an added keyword multi-field is applied by a zero-downtime reindex"() {
    given: 'a third documents alias with the sample set'
    IndexNamingStrategy mfNaming = IndexNamingStrategy.prefixed(prefix + '-mf')
    def plain = ProtoMapper.builder().naming(mfNaming).build()
    def withKeyword = ProtoMapper.builder().naming(mfNaming).overrides(MappingOverrides.builder()
        .forField(Document.descriptor, 'body') { it.setKeywordSubfield(Bool3.TRUE) }.build()).build()
    IndexDefinition v1 = plain.map(Document)
    IndexDefinition v2 = withKeyword.map(Document)
    indexSampleDocuments(plain)
    def migrator = IndexMigrator.builder().engine(engine).mapper(withKeyword)
        .options(FAST.withBackfill(false)).build()

    when:
    MigrationPlan plan = migrator.plan(Document)
    def result = migrator.apply(plan)

    then:
    plan.items()*.action() == [Action.REINDEX]
    plan.items()[0].reason().endsWith('(backfill is disabled)')
    result.ok()
    result.outcomes()[0].index() == v2.name()
    result.outcomes()[0].retainedIndices() == [v1.name()]
    result.outcomes()[0].sourceCount() == 6L
    result.outcomes()[0].destCount() == 6L
    result.outcomes()[0].reconcile() == new ReconcileStats(0, 0, 0, 0)
    engine.getAliases(v1.alias()).keySet() == [v2.name()] as Set
    engine.getMappings(v2.name())._meta.elastic_protos.keySet() == ['hash', 'proto', 'schema_version', 'static_settings'] as Set
    ids(engine.search([v1.alias()], [query: [term: ['body.keyword':
        'Counsel enters an appearance on behalf of the defendant.']]])) == ['doc-4'] as Set
    migrator.plan(Document).items()*.action() == [Action.NO_OP]
  }

  def "a changed field type is applied by a zero-downtime reindex while writes go on through the alias"() {
    given:
    def overrides = MappingOverrides.builder()
        .forField(Document.descriptor, 'body') { it.setKeywordSubfield(Bool3.TRUE) }
        .forField(Document.descriptor, 'title') { it.setType(EsType.KEYWORD) }
        .build()
    def breakingMapper = ProtoMapper.builder().naming(naming).overrides(overrides).build()
    MigrationScriptProvider script = { alias, from, to -> Optional.of(MIGRATION_SCRIPT) } as MigrationScriptProvider
    String alias = documents.alias()
    Map<Phase, Closure> writes = [
        (Phase.COPIED)   : { write(alias, 'zd-1', 'copy window'); engine.delete(alias, 'zd-2'); write(alias, 'zd-5', 'copy window') },
        (Phase.CAUGHT_UP): { write(alias, 'zd-1', 'catch-up 1'); write(alias, 'zd-1', 'catch-up 2'); engine.delete(alias, 'zd-3'); write(alias, 'zd-6', 'catch-up window'); write(alias, 'zd-9', 'catch-up window') },
        (Phase.SWAPPED)  : { write(alias, 'zd-1', 'after swap'); engine.delete(alias, 'zd-4'); write(alias, 'zd-7', 'after swap'); write(alias, 'zd-5', 'after swap'); engine.delete(alias, 'zd-9') },
    ]
    def breaking = IndexMigrator.builder().engine(engine).mapper(breakingMapper).scriptProvider(script).options(FAST)
        .listener({ a, phase -> writes[phase]?.call() } as MigrationListener).build()
    IndexDefinition target = breakingMapper.map(Document)

    and: 'five more documents written before the migration'
    ['zd-1', 'zd-2', 'zd-3', 'zd-4', 'zd-9'].each { write(alias, it, 'before') }
    engine.refresh(alias)

    when:
    MigrationPlan plan = breaking.plan(Document)

    then:
    plan.items()*.action() == [Action.REINDEX]
    plan.items()[0].currentIndex() == originalDocumentIndex
    plan.items()[0].target().name() == target.name()
    target.name() != originalDocumentIndex

    when: 'the reindex runs while the listener writes through the alias after the copy, the catch-up and the swap'
    def result = breaking.apply(plan)
    reindexPlan = plan.items()[0]
    engine.refresh(alias)

    then: 'no write failed and the alias points only at the new index'
    result.ok()
    result.outcomes()[0].index() == target.name()
    result.outcomes()[0].retainedIndices() == [originalDocumentIndex]
    engine.getAliases(alias).keySet() == [target.name()] as Set
    engine.getMappings(alias).properties.title.type == 'keyword'
    engine.getMappings(target.name())._meta.elastic_protos.keySet() == ['hash', 'proto', 'schema_version', 'static_settings'] as Set

    and: 'the last write wins per document and nothing deleted comes back, including zd-9, updated in the old index after the catch-up and deleted in the new one after the swap'
    titles(target.name()) == SampleData.documents().collectEntries { [(it.id): it.title] } + [
        'zd-1': 'after swap', 'zd-5': 'after swap', 'zd-6': 'catch-up window', 'zd-7': 'after swap']
    result.outcomes()[0].reconcile() == new ReconcileStats(1, 3, 2, 0)
    result.outcomes()[0].sourceCount() == 11L
    result.outcomes()[0].destCount() == 10L

    and: 'the migration script ran on every copied document, including the one reconciled through the staging index'
    docIds(where(fc('tags', EQUALS, str('migrated'))), target) == (SampleData.documents()*.id + ['zd-6']) as Set
    !engine.indexExists(target.name() + '-reconcile')

    and: 'title is now a keyword'
    docIds(where(fc('title', EQUALS, str('Motion to dismiss'))), target) == ['doc-2'] as Set
    docIds(where(fc('title', TEXT_PREFIX, str('Order '))), target) == ['doc-3'] as Set

    when: 'writing to the retained old index'
    engine.index(originalDocumentIndex, 'doc-x', codec.toDocument(SampleData.documents()[0]), IndexOptions.defaults())

    then: 'it is write-blocked, through _block/write, which reads back as index.blocks.write'
    thrown(EngineException)
    engine.indexExists(originalDocumentIndex)
    engine.getSettings(originalDocumentIndex).index.blocks.write == 'true'
  }

  def "a zero-downtime reindex that dies after the swap is resumed by the next run"() {
    given: 'a fourth documents alias with the sample set'
    IndexNamingStrategy crashNaming = IndexNamingStrategy.prefixed(prefix + '-crash')
    def plain = ProtoMapper.builder().naming(crashNaming).build()
    def titleKeyword = ProtoMapper.builder().naming(crashNaming).overrides(MappingOverrides.builder()
        .forField(Document.descriptor, 'title') { it.setType(EsType.KEYWORD) }.build()).build()
    IndexDefinition v1 = plain.map(Document)
    IndexDefinition v2 = titleKeyword.map(Document)
    String alias = v1.alias()
    indexSampleDocuments(plain)
    Map<Phase, Closure> on = [
        (Phase.CAUGHT_UP): { write(alias, 'crash-0', 'after the catch-up') },
        (Phase.FROZEN)   : { throw new MigrationListener.Abort('killed') },
    ]
    def crashing = IndexMigrator.builder().engine(engine).mapper(titleKeyword).options(FAST)
        .listener({ a, phase -> on[phase]?.call() } as MigrationListener).build()

    when: 'the run dies once the old index is frozen'
    def crashed = crashing.bootstrap(Document)

    then: 'the alias already points at the new index, which records the phase'
    crashed.outcomes()[0].status() == MigrationResult.Status.FAILED
    engine.getAliases(alias).keySet() == [v2.name()] as Set
    engine.getMappings(v2.name())._meta.elastic_protos.pending.phase == 'reconciling'
    engine.getMappings(v2.name())._meta.elastic_protos.pending.from == v1.name()

    when: 'writes go on, and a new process plans and applies'
    write(alias, 'crash-1', 'after the crash')
    def resumer = IndexMigrator.builder().engine(engine).mapper(titleKeyword).options(FAST).build()
    MigrationPlan plan = resumer.plan(Document)
    def result = resumer.apply(plan)
    engine.refresh(alias)

    then:
    plan.items()*.action() == [Action.REINDEX]
    plan.items()[0].resume().swapped()
    result.ok()
    result.outcomes()[0].reconcile().copied() == 1L
    titles(v2.name()) == SampleData.documents().collectEntries { [(it.id): it.title] } + [
        'crash-0': 'after the catch-up', 'crash-1': 'after the crash']
    engine.getMappings(v2.name())._meta.elastic_protos.pending == null
    resumer.plan(Document).items()*.action() == [Action.NO_OP]
    outcome { engine.index(v1.name(), 'x', [id: 'x'], IndexOptions.defaults()) } == EngineException.Kind.BLOCKED
  }

  def "a zero-downtime reindex that dies right after the swap, before the freeze, is resumed"() {
    given:
    IndexNamingStrategy crashNaming = IndexNamingStrategy.prefixed(prefix + '-crash2')
    def plain = ProtoMapper.builder().naming(crashNaming).build()
    def titleKeyword = ProtoMapper.builder().naming(crashNaming).overrides(MappingOverrides.builder()
        .forField(Document.descriptor, 'title') { it.setType(EsType.KEYWORD) }.build()).build()
    IndexDefinition v1 = plain.map(Document)
    IndexDefinition v2 = titleKeyword.map(Document)
    String alias = v1.alias()
    indexSampleDocuments(plain)
    Map<Phase, Closure> on = [
        (Phase.CAUGHT_UP): { write(alias, 'crash-0', 'after the catch-up') },
        (Phase.SWAPPED)  : { throw new MigrationListener.Abort('killed') },
    ]
    def crashing = IndexMigrator.builder().engine(engine).mapper(titleKeyword).options(FAST)
        .listener({ a, phase -> on[phase]?.call() } as MigrationListener).build()

    when:
    def crashed = crashing.bootstrap(Document)

    then: 'the old index was never blocked and the marker still says swapping'
    crashed.outcomes()[0].status() == MigrationResult.Status.FAILED
    engine.getAliases(alias).keySet() == [v2.name()] as Set
    engine.getMappings(v2.name())._meta.elastic_protos.pending.phase == 'swapping'
    engine.getSettings(v1.name()).index.blocks?.write != 'true'

    when:
    def resumer = IndexMigrator.builder().engine(engine).mapper(titleKeyword).options(FAST).build()
    def result = resumer.bootstrap(Document)

    then:
    result.ok()
    result.outcomes()[0].plan().resume().swapped()
    titles(v2.name()) == SampleData.documents().collectEntries { [(it.id): it.title] } + ['crash-0': 'after the catch-up']
    engine.getSettings(v1.name()).index.blocks.write == 'true'
    engine.getMappings(v2.name())._meta.elastic_protos.pending == null
  }

  def "a reverse-delta rollback restores the old index with the writes made after the reindex"() {
    given:
    String alias = documents.alias()
    String next = reindexPlan.target().name()
    write(alias, 'zd-8', 'after the reindex')

    when:
    def rolledBack = IndexMigrator.builder().engine(engine).mapper(mapper).options(FAST).build().rollback(reindexPlan)
    engine.refresh(alias)

    then:
    rolledBack.ok()
    rolledBack.index() == originalDocumentIndex
    rolledBack.retainedIndices() == [next]
    engine.getAliases(alias).keySet() == [originalDocumentIndex] as Set
    titles(originalDocumentIndex) == titles(next)
    titles(originalDocumentIndex)['zd-8'] == 'after the reindex'
    titles(originalDocumentIndex)['zd-1'] == 'after swap'
    count(alias) == 11
    engine.getMappings(originalDocumentIndex)._meta.elastic_protos.keySet() == ['hash', 'proto', 'schema_version', 'static_settings'] as Set
    outcome { engine.index(next, 'x', [id: 'x'], IndexOptions.defaults()) } == EngineException.Kind.BLOCKED

    when: 'the restored index takes writes'
    def written = engine.index(originalDocumentIndex, 'doc-x',
        codec.toDocument(SampleData.documents()[0].toBuilder().setId('doc-x').build()), IndexOptions.refreshed())

    then:
    written != null
    count(alias) == 12
  }

  // ------------------------------------------------------------------ settings

  def "dynamic settings are updated in place, and a static change reindexes into the next generation"() {
    given: 'a documents alias under -st with the sample set'
    IndexNamingStrategy stNaming = IndexNamingStrategy.prefixed(prefix + '-st')
    def plain = ProtoMapper.builder().naming(stNaming).build()
    IndexDefinition v1 = plain.map(Document)
    indexSampleDocuments(plain)
    def dynamic = ProtoMapper.builder().naming(stNaming).overrides(MappingOverrides.builder()
        .setting('refresh_interval', '5s').setting('index.max_result_window', 20000).build()).build()
    def migrator = IndexMigrator.builder().engine(engine).mapper(dynamic).options(FAST).build()

    when:
    MigrationPlan plan = migrator.plan(Document)
    def result = migrator.apply(plan)
    def settings = engine.getSettings(v1.name()).index

    then:
    plan.items()*.action() == [Action.UPDATE_SETTINGS]
    result.ok()
    settings.refresh_interval == '5s'
    settings.max_result_window == '20000'
    migrator.plan(Document).items()*.action() == [Action.NO_OP]

    when: 'the shard count changes, with static settings reindexed'
    def sharded = ProtoMapper.builder().naming(stNaming).overrides(MappingOverrides.builder()
        .setting('refresh_interval', '5s').setting('index.max_result_window', 20000)
        .setting('number_of_shards', 2).build()).build()
    def reindexing = IndexMigrator.builder().engine(engine).mapper(sharded)
        .options(FAST.withStaticSettings(StaticSettingsPolicy.REINDEX)).build()
    MigrationPlan staticPlan = reindexing.plan(Document)
    def reindexed = reindexing.apply(staticPlan)
    def newSettings = engine.getSettings(v1.name() + '-g2').index

    then:
    staticPlan.items()*.action() == [Action.REINDEX]
    staticPlan.items()[0].target().name() == v1.name() + '-g2'
    staticPlan.items()[0].reason().startsWith('static settings changed: number_of_shards 1 -> 2')
    reindexed.ok()
    reindexed.outcomes()[0].sourceCount() == 6L
    reindexed.outcomes()[0].destCount() == 6L
    engine.getAliases(v1.alias()).keySet() == [v1.name() + '-g2'] as Set
    newSettings.number_of_shards == '2'
    newSettings.refresh_interval == '5s'
    engine.getMappings(v1.name() + '-g2')._meta.elastic_protos.static_settings ==
        DiffClassifier.staticSettingsDigest(sharded.map(Document).settings(), [] as Set)
    reindexing.plan(Document).items()*.action() == [Action.NO_OP]
  }

  // ------------------------------------------------------------------ concurrent bootstrap

  def "a second process waits for the lock while the first migrates, then re-plans to NO_OP"() {
    given: 'an alias under -lk with the sample set, and a mapper that backfills a keyword sub-field'
    def plain = ProtoMapper.builder().naming(IndexNamingStrategy.prefixed(prefix + '-lk')).build()
    def withKeyword = ProtoMapper.builder().naming(IndexNamingStrategy.prefixed(prefix + '-lk'))
        .overrides(MappingOverrides.builder().forField(Document.descriptor, 'body') { it.setKeywordSubfield(Bool3.TRUE) }.build())
        .build()
    indexSampleDocuments(plain)
    String locks = plain.naming().lockIndex()
    def leaderHolds = new java.util.concurrent.CountDownLatch(1)
    def release = new java.util.concurrent.CountDownLatch(1)
    def leader = IndexMigrator.builder().engine(engine).mapper(withKeyword).listener({ a, phase ->
      if (phase == Phase.BACKFILL_MARKED) {
        leaderHolds.countDown()
        release.await()
      }
    } as MigrationListener).build()
    def follower = IndexMigrator.builder().engine(engine).mapper(withKeyword).build()
    def results = [:].asSynchronized()

    when: 'the leader takes the lock and stops inside its backfill'
    def leaderThread = Thread.start { results.leader = leader.bootstrap(Document) }
    assert leaderHolds.await(60, java.util.concurrent.TimeUnit.SECONDS)
    def followerThread = Thread.start { results.follower = follower.bootstrap(Document) }
    followerThread.join(3000)
    boolean followerWaited = followerThread.isAlive()
    release.countDown()
    leaderThread.join(120_000)
    followerThread.join(120_000)
    engine.refresh(locks)

    then: 'the follower planned the backfill too, waited, and found it done under the lock'
    followerWaited
    results.leader.ok()
    results.leader.outcomes()[0].plan().action() == Action.BACKFILL
    results.follower.ok()
    results.follower.outcomes()[0].plan().action() == Action.NO_OP
    results.follower.outcomes()[0].status() == MigrationResult.Status.SKIPPED
    count(locks) == 0
    engine.getMappings(locks).dynamic == 'strict'
  }

  def "an expired lock left by a dead process is taken over"() {
    given:
    def lkMapper = ProtoMapper.builder().naming(IndexNamingStrategy.prefixed(prefix + '-lk')).build()
    String locks = lkMapper.naming().lockIndex()
    String alias = lkMapper.map(Firm).alias()
    engine.index(locks, alias, [alias: alias, holder: 'dead-host/1/deadbeef', action: 'CREATE', hash: 'x',
                                acquired_at: '2026-01-01T00:00:00Z', expires_at: Instant.now().minusSeconds(300).toString()],
        IndexOptions.refreshed())

    when:
    def result = IndexMigrator.builder().engine(engine).mapper(lkMapper)
        .options(MigrationOptions.standard().withLockWait(Duration.ZERO)).build().bootstrap(Firm)
    engine.refresh(locks)

    then:
    result.ok()
    result.outcomes()[0].plan().action() == Action.CREATE
    !engine.get(locks, alias).isPresent()
  }

  def "a lock another process holds makes the item LOCKED once lockWait has passed"() {
    given:
    def lkMapper = ProtoMapper.builder().naming(IndexNamingStrategy.prefixed(prefix + '-lk')).build()
    String locks = lkMapper.naming().lockIndex()
    String alias = lkMapper.map(InboxItem).alias()
    engine.index(locks, alias, [alias: alias, holder: 'busy-host/2/cafebabe', action: 'REINDEX', hash: 'x',
                                acquired_at: Instant.now().toString(), expires_at: Instant.now().plusSeconds(600).toString()],
        IndexOptions.refreshed())

    when:
    def result = IndexMigrator.builder().engine(engine).mapper(lkMapper)
        .options(MigrationOptions.standard().withLockWait(Duration.ZERO)).build().bootstrap(InboxItem)

    then:
    !result.ok()
    result.outcomes()[0].status() == MigrationResult.Status.LOCKED
    result.outcomes()[0].error().message.contains('busy-host/2/cafebabe')
    !engine.indexExists(alias)

    cleanup:
    engine.delete(locks, alias, IndexOptions.refreshed())
  }

  // ------------------------------------------------------------------ errors

  def "engine errors are classified"() {
    when:
    engine.createIndex(originalDocumentIndex, [:], documents.mappings(), [] as Set)

    then:
    def exists = thrown(EngineException)
    exists.kind == EngineException.Kind.ALREADY_EXISTS

    when:
    engine.getMappings(prefix + '-missing')

    then:
    def missing = thrown(EngineException)
    missing.kind == EngineException.Kind.NOT_FOUND
  }

  // ------------------------------------------------------------------ engine SPI

  def "settings updates, conditional writes, hit metadata, update by query and external-version reindex"() {
    given: 'two indices of their own, so the sample set is untouched'
    String spi = prefix + '-spi'
    String copy = prefix + '-spi-copy'
    Map<String, Object> settings = [number_of_shards: 1, number_of_replicas: 0]
    Map<String, Object> mappings = [properties: [title: [type: 'text']]]
    engine.createIndex(spi, settings, mappings, [] as Set)
    engine.createIndex(copy, settings, mappings, [] as Set)

    when: 'a dynamic setting is changed'
    engine.putSettings(spi, ['index.refresh_interval': '5s'])

    then:
    engine.getSettings(spi).index.refresh_interval == '5s'

    when: 'a document is written, then overwritten on its sequence number and primary term'
    def first = engine.index(spi, '1', [title: 'one'], IndexOptions.defaults())
    def second = engine.index(spi, '1', [title: 'uno'], IndexOptions.ifMatch(first.seqNo(), first.primaryTerm()))

    then:
    first.seqNo() != null
    first.primaryTerm() != null
    second.seqNo() > first.seqNo()
    second.version() == 2

    when: 'the sequence number is stale'
    engine.index(spi, '1', [title: 'stale'], IndexOptions.ifMatch(first.seqNo(), first.primaryTerm()))

    then:
    def conflict = thrown(EngineException)
    conflict.kind == EngineException.Kind.CONFLICT
    conflict.status == 409

    when: 'a search asks for versions and sequence numbers, and get returns them anyway'
    engine.bulk([BulkOp.index(spi, '2', [title: 'two']), BulkOp.index(spi, '3', [title: 'three'])])
    engine.refresh(spi)
    def hits = engine.search([spi], [query: [ids: [values: ['1']]], version: true, seq_no_primary_term: true]).hits()
    def got = engine.get(spi, '1').get()

    then:
    hits*.id() == ['1']
    hits[0].version() == 2L
    hits[0].seqNo() == second.seqNo()
    hits[0].primaryTerm() == second.primaryTerm()
    got.version() == 2L
    got.seqNo() == second.seqNo()
    got.primaryTerm() == second.primaryTerm()

    when: 'every document is updated in place'
    long docs = count(spi)
    def updated = engine.updateByQuery(UpdateByQueryRequest.builder().index(spi).proceedOnConflicts(true).build())

    then:
    docs == 3
    updated.total() == docs
    updated.updated() == docs
    updated.accounted()
    updated.ok()
    engine.get(spi, '1').get().version() == 3L

    when: 'copied twice with external versions'
    def external = ReindexRequest.of(spi, copy)
        .withVersionType(ReindexRequest.VersionType.EXTERNAL).withProceedOnConflicts(true)
    def firstCopy = engine.reindex(external)
    def secondCopy = engine.reindex(external)

    then: 'the second copy finds nothing newer and conflicts on every document'
    firstCopy.created() == docs
    firstCopy.accounted()
    engine.get(copy, '1').get().version() == 3L
    secondCopy.total() == docs
    secondCopy.versionConflicts() == secondCopy.total()
    secondCopy.created() == 0
    secondCopy.updated() == 0
    secondCopy.accounted()
    secondCopy.ok()

    when: 'without proceeding on conflicts'
    def aborted = engine.reindex(external.withProceedOnConflicts(false))

    then: 'the conflict is reported as a failure, and the total still counts every document'
    !aborted.ok()
    aborted.versionConflicts() >= 1
    !aborted.failures().isEmpty()
    !aborted.complete()

    when: 'a conditional delete has a stale sequence number'
    def two = engine.get(spi, '2').get()
    engine.delete(spi, '2', IndexOptions.ifMatch(two.seqNo() + 100, two.primaryTerm()))

    then:
    def staleDelete = thrown(EngineException)
    staleDelete.kind == EngineException.Kind.CONFLICT
    engine.get(spi, '2').present

    when: 'a conditional delete names a missing document'
    def missingDelete = outcome { engine.delete(spi, 'missing', IndexOptions.ifMatch(0, 1)) }

    then: 'both engines answer 409'
    missingDelete == EngineException.Kind.CONFLICT

    when: 'a bulk carries one stale conditional delete'
    def bulk = engine.bulk([
        BulkOp.index(spi, '4', [title: 'four']),
        BulkOp.deleteIfMatch(spi, '2', two.seqNo() + 100, two.primaryTerm()),
        BulkOp.deleteIfMatch(spi, '3', engine.get(spi, '3').get().seqNo(), two.primaryTerm()),
    ])

    then: 'only that item fails'
    bulk.items()*.status() == [201, 409, 200]
    bulk.failures()*.id() == ['2']
    engine.get(spi, '2').present
    !engine.get(spi, '3').present

    when: 'a reindex selects one document by id'
    String one = prefix + '-spi-one'
    engine.createIndex(one, settings, mappings, [] as Set)
    def selected = engine.reindex(ReindexRequest.of(spi, one).withQuery([ids: [values: ['1']]]))

    then:
    selected.total() == 1
    selected.created() == 1
    selected.complete()
    count(one) == 1

    when: 'a match query asks for highlights on a text field'
    engine.refresh(spi)
    def highlighted = engine.search([spi], [query: [match: [title: 'four']], highlight: [fields: [title: [:]]]]).hits()

    then:
    highlighted*.id() == ['4']
    !highlighted[0].highlight()['title'].isEmpty()
    highlighted[0].highlight()['title'][0].contains('<em>four</em>')
  }

  /** Runs a call and returns its result, or the kind of the {@link EngineException} it threw. */
  static Object outcome(Closure<?> call) {
    try {
      call()
    } catch (EngineException e) {
      e.kind
    }
  }

  // ------------------------------------------------------------------ helpers

  long count(String index) {
    engine.count(index)
  }

  /** Writes a document with an id and a title through an alias or index. */
  void write(String target, String id, String title) {
    engine.index(target, id, codec.toDocument(SampleData.documents()[0].toBuilder().setId(id).setTitle(title).build()),
        IndexOptions.defaults())
  }

  /** Returns the title of every document of an index, by id. */
  Map<String, String> titles(String index) {
    engine.refresh(index)
    engine.search([index], [query: [match_all: [:]], size: 100]).hits().collectEntries { [(it.id()): it.source().title] }
  }

  /** Bootstraps Document with a mapper and bulk indexes the sample set through its alias. */
  void indexSampleDocuments(ProtoMapper with) {
    assert IndexMigrator.builder().engine(engine).mapper(with).build().bootstrap(Document).ok()
    def withCodec = ProtoDocumentCodec.of(with)
    def bulk = engine.bulk(SampleData.documents().collect {
      BulkOp.index(with.map(Document).alias(), withCodec.idOf(it).get(), withCodec.toDocument(it))
    })
    assert !bulk.errors()
    engine.refresh(with.map(Document).alias())
  }

  Set<String> docIds(SearchCriteria criteria, IndexDefinition index = documents) {
    def compiled = compiler.compile(criteria, index)
    assert compiled.warnings().isEmpty()
    ids(engine.search([index.alias()], compiled.body()))
  }

  Set<String> firmIds(SearchCriteria criteria) {
    docIds(criteria, firms)
  }

  AggregationResults aggregate(IndexDefinition index, Criteria criteria) {
    def compiled = compiler.compile(criteria.build(), index)
    assert compiled.warnings().isEmpty()
    AggregationResults.of(engine.search([index.alias()], compiled.body()))
  }

  List<String> orderedIds(IndexDefinition index, SearchCriteria criteria) {
    engine.search([index.alias()], compiler.compile(criteria, index).body()).hits()*.id()
  }

  static Set<String> ids(SearchResult result) {
    result.hits()*.id() as Set
  }

  static FlatValue str(String s) { FlatValue.newBuilder().setStringValue(s).build() }

  static FlatValue integer(long l) { FlatValue.newBuilder().setIntValue(l).build() }

  static FlatValue range(double min, double max, boolean minInc = true, boolean maxInc = true) {
    FlatValue.newBuilder().setRangeValue(RangeValue.newBuilder()
        .setMin(min).setMax(max).setMinInclusive(minInc).setMaxInclusive(maxInc)).build()
  }

  static FlatValue list(String... values) {
    FlatValue.newBuilder().setListValue(PrimitiveList.newBuilder().addAllValues(
        values.collect { PrimitiveValue.newBuilder().setStringValue(it).build() })).build()
  }

  static FieldCondition fc(String field, SearchOperationType op,
      FlatValue value = FlatValue.defaultInstance, boolean negate = false) {
    FieldCondition.newBuilder().setField(field).setOperation(op).setValue(value).setNegate(negate).build()
  }

  static FieldCondition fullTextOn(String field, String query) {
    FieldCondition.newBuilder().setField(field).setOperation(SearchOperationType.FULL_TEXT)
        .setValue(str(query)).build()
  }

  static SearchCriteria where(FieldCondition... conditions) {
    SearchCriteria.newBuilder().addSearchCondition(
        SearchCondition.newBuilder().addAllFieldCondition(conditions as List)).build()
  }

  static SortingCriteria fieldSort(String field, SortingOrder order) {
    SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder().setFieldName(field).setOrder(order)).build()
  }

  static SearchCriteria sorted(SortingCriteria... sorts) {
    SearchCriteria.newBuilder().addAllSortingCriteria(sorts as List).build()
  }

  static SearchCriteria paged(Pagination pagination, SearchCriteria base) {
    base.toBuilder().setPagination(pagination).build()
  }
}
