package com.phatjam98.elasticprotos.testkit

import static com.phatjam98.elasticprotos.proto.SearchOperationType.ANY_IN
import static com.phatjam98.elasticprotos.proto.SearchOperationType.EQUALS
import static com.phatjam98.elasticprotos.proto.SearchOperationType.EXISTS
import static com.phatjam98.elasticprotos.proto.SearchOperationType.NUM_RANGE
import static com.phatjam98.elasticprotos.proto.SearchOperationType.TEXT_CONTAINS
import static com.phatjam98.elasticprotos.proto.SearchOperationType.TEXT_MATCHES
import static com.phatjam98.elasticprotos.proto.SearchOperationType.TEXT_PREFIX
import static com.phatjam98.elasticprotos.proto.SearchOperationType.TEXT_REGEX

import com.google.protobuf.Message
import com.phatjam98.elasticprotos.codec.ProtoDocumentCodec
import com.phatjam98.elasticprotos.engine.BulkOp
import com.phatjam98.elasticprotos.engine.EngineException
import com.phatjam98.elasticprotos.engine.EngineProduct
import com.phatjam98.elasticprotos.engine.IndexOptions
import com.phatjam98.elasticprotos.engine.SearchEngine
import com.phatjam98.elasticprotos.engine.SearchResult
import com.phatjam98.elasticprotos.mapping.IndexDefinition
import com.phatjam98.elasticprotos.mapping.IndexNamingStrategy
import com.phatjam98.elasticprotos.mapping.MappingOverrides
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.migration.Action
import com.phatjam98.elasticprotos.migration.DiffClassifier
import com.phatjam98.elasticprotos.migration.IndexMigrator
import com.phatjam98.elasticprotos.migration.IndexPlan
import com.phatjam98.elasticprotos.migration.MigrationPlan
import com.phatjam98.elasticprotos.migration.MigrationResult
import com.phatjam98.elasticprotos.migration.MigrationScriptProvider
import com.phatjam98.elasticprotos.proto.Bool3
import com.phatjam98.elasticprotos.proto.DistanceUnit
import com.phatjam98.elasticprotos.proto.EsType
import com.phatjam98.elasticprotos.proto.FieldCondition
import com.phatjam98.elasticprotos.proto.FieldSort
import com.phatjam98.elasticprotos.proto.FlatValue
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
import com.phatjam98.elasticprotos.query.Criteria
import com.phatjam98.elasticprotos.query.QueryCompiler
import com.phatjam98.elasticprotos.query.QueryCompilerConfig
import com.phatjam98.elasticprotos.query.SearchPager
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
@Requires(value = { DockerClientFactory.instance().isDockerAvailable() }, inherited = true,
    reason = 'Docker is not available')
abstract class EngineIntegrationSpec extends Specification {

  /** The script applied by the breaking migration: tags every copied document. */
  static final String MIGRATION_SCRIPT =
      "if (ctx._source.tags == null) { ctx._source.tags = []; } ctx._source.tags.add('migrated')"

  static final int PAGING_DOCS = 2500

  @Shared SearchEngine engine
  @Shared String prefix = 'it-' + UUID.randomUUID().toString().substring(0, 8)
  @Shared IndexNamingStrategy naming = IndexNamingStrategy.prefixed(prefix)
  @Shared ProtoMapper mapper = ProtoMapper.builder().naming(naming).build()
  @Shared ProtoDocumentCodec codec = ProtoDocumentCodec.of(mapper)
  @Shared QueryCompiler compiler = QueryCompiler.withDefaults()
  @Shared IndexMigrator migrator
  @Shared IndexDefinition firms = mapper.map(Firm)
  @Shared IndexDefinition documents = mapper.map(Document)

  // state carried between the ordered features
  @Shared String originalDocumentIndex
  @Shared IndexDefinition additiveDocuments
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

    and: 'map keys the engine picked up (Firm.labels.tier) are not removed fields'
    engine.getMappings(firms.alias()).properties.labels.properties.tier != null
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

  // ------------------------------------------------------------------ migrations

  def "an added keyword sub-field is applied in place with PUT_MAPPING"() {
    given:
    def overrides = MappingOverrides.builder()
        .forField(Document.descriptor, 'body') { it.setKeywordSubfield(Bool3.TRUE) }
        .build()
    def additiveMapper = ProtoMapper.builder().naming(naming).overrides(overrides).build()
    def additive = IndexMigrator.builder().engine(engine).mapper(additiveMapper).build()
    additiveDocuments = additiveMapper.map(Document)

    when:
    MigrationPlan plan = additive.plan(Document)

    then:
    plan.items()*.action() == [Action.PUT_MAPPING]
    plan.items()[0].diff().added()*.path() == ['body.keyword']
    additiveDocuments.contentHash() != documents.contentHash()

    when:
    def result = additive.apply(plan)
    def live = engine.getMappings(documents.alias())

    then:
    result.ok()
    result.outcomes()[0].index() == originalDocumentIndex
    engine.getAliases(documents.alias()).keySet() == [originalDocumentIndex] as Set
    live._meta.elastic_protos.hash == additiveDocuments.contentHash()
    live.properties.body.fields.keyword.type == 'keyword'
    additive.plan(Document).items()*.action() == [Action.NO_OP]
  }

  def "a changed field type is applied by reindexing behind the alias"() {
    given:
    def overrides = MappingOverrides.builder()
        .forField(Document.descriptor, 'body') { it.setKeywordSubfield(Bool3.TRUE) }
        .forField(Document.descriptor, 'title') { it.setType(EsType.KEYWORD) }
        .build()
    def breakingMapper = ProtoMapper.builder().naming(naming).overrides(overrides).build()
    MigrationScriptProvider script = { alias, from, to -> Optional.of(MIGRATION_SCRIPT) } as MigrationScriptProvider
    def breaking = IndexMigrator.builder().engine(engine).mapper(breakingMapper).scriptProvider(script).build()
    IndexDefinition target = breakingMapper.map(Document)

    when:
    MigrationPlan plan = breaking.plan(Document)

    then:
    plan.items()*.action() == [Action.REINDEX]
    plan.items()[0].currentIndex() == originalDocumentIndex
    plan.items()[0].target().name() == target.name()
    target.name() != originalDocumentIndex

    when:
    def result = breaking.apply(plan)
    reindexPlan = plan.items()[0]

    then: 'the alias points only at the new index and nothing was lost'
    result.ok()
    result.outcomes()[0].index() == target.name()
    result.outcomes()[0].retainedIndices() == [originalDocumentIndex]
    engine.getAliases(documents.alias()).keySet() == [target.name()] as Set
    count(documents.alias()) == 6
    count(originalDocumentIndex) == 6
    engine.getMappings(documents.alias()).properties.title.type == 'keyword'

    and: 'the migration script ran on every document'
    docIds(where(fc('tags', EQUALS, str('migrated'))), target) == SampleData.documents()*.id as Set

    and: 'title is now a keyword'
    docIds(where(fc('title', EQUALS, str('Motion to dismiss'))), target) == ['doc-2'] as Set
    docIds(where(fc('title', TEXT_PREFIX, str('Order '))), target) == ['doc-3'] as Set

    when: 'writing to the retained old index'
    engine.index(originalDocumentIndex, 'doc-x', codec.toDocument(SampleData.documents()[0]), IndexOptions.defaults())

    then: 'it is write-blocked'
    thrown(EngineException)
    engine.indexExists(originalDocumentIndex)
  }

  def "rollback points the alias back at the old index and unblocks it"() {
    when:
    IndexMigrator.builder().engine(engine).mapper(mapper).build().rollback(reindexPlan)

    then:
    engine.getAliases(documents.alias()).keySet() == [originalDocumentIndex] as Set
    count(documents.alias()) == 6

    when:
    def written = engine.index(originalDocumentIndex, 'doc-x',
        codec.toDocument(SampleData.documents()[0].toBuilder().setId('doc-x').build()), IndexOptions.refreshed())

    then:
    written != null
    count(documents.alias()) == 7
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

  // ------------------------------------------------------------------ helpers

  long count(String index) {
    engine.count(index)
  }

  Set<String> docIds(SearchCriteria criteria, IndexDefinition index = documents) {
    def compiled = compiler.compile(criteria, index)
    assert compiled.warnings().isEmpty()
    ids(engine.search([index.alias()], compiled.body()))
  }

  Set<String> firmIds(SearchCriteria criteria) {
    docIds(criteria, firms)
  }

  List<String> orderedIds(IndexDefinition index, SearchCriteria criteria) {
    engine.search([index.alias()], compiler.compile(criteria, index).body()).hits()*.id()
  }

  static Set<String> ids(SearchResult result) {
    result.hits()*.id() as Set
  }

  static FlatValue str(String s) { FlatValue.newBuilder().setStringValue(s).build() }

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
