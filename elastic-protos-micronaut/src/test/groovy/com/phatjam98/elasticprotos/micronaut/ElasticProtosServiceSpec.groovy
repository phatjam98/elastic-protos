package com.phatjam98.elasticprotos.micronaut

import com.phatjam98.elasticprotos.engine.BulkOp
import com.phatjam98.elasticprotos.engine.BulkResult
import com.phatjam98.elasticprotos.engine.EngineException
import com.phatjam98.elasticprotos.engine.IndexOptions
import com.phatjam98.elasticprotos.migration.Action
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.proto.example.FirmStatus
import com.phatjam98.elasticprotos.query.AggregationResults
import com.phatjam98.elasticprotos.query.Aggs
import com.phatjam98.elasticprotos.query.Criteria
import com.phatjam98.elasticprotos.query.QueryCompiler
import com.phatjam98.elasticprotos.query.QueryCompilerConfig
import com.phatjam98.elasticprotos.query.TrackTotalHits
import com.phatjam98.elasticprotos.query.QueryCompileException
import com.phatjam98.elasticprotos.testkit.InMemorySearchEngine
import com.phatjam98.elasticprotos.testkit.SampleData
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import java.util.function.BiConsumer
import java.util.function.Consumer
import spock.lang.Specification

@MicronautTest(startApplication = false)
@Property(name = 'test.engine', value = 'memory')
@Property(name = 'elastic-protos.bootstrap.classes',
    value = 'com.phatjam98.elasticprotos.proto.example.Firm,com.phatjam98.elasticprotos.proto.example.Document')
@Property(name = 'elastic-protos.bulk.flush-interval', value = '0s')
@Property(name = 'elastic-protos.bulk.actions', value = '2')
class ElasticProtosServiceSpec extends Specification {

  @Inject
  ElasticProtosService service

  @Inject
  InMemorySearchEngine engine

  def setup() {
    engine.indices().keySet().each { index ->
      engine.documents(index).keySet().each { id -> engine.delete(index, id) }
    }
  }

  def "the startup bootstrap created both aliases on the injected engine"() {
    expect:
    service.engine().is(engine)
    engine.indices().values().flatten() as Set == ['firms', 'documents'] as Set
  }

  def "definition returns the cached mapping of a type"() {
    when:
    def definition = service.definition(Firm)

    then:
    definition.alias() == 'firms'
    definition.idField() == 'id'
    service.definition(Firm).is(definition)
  }

  def "index then get round-trips a message"() {
    given:
    Firm firm = SampleData.firms()[0]

    when:
    def result = service.index(firm)

    then:
    result.id() == 'firm-1'
    result.index().startsWith('firms-v')
    service.get('firm-1', Firm).get() == firm
    service.get('nope', Firm).isEmpty()
  }

  def "get reads through the realtime document API, not a search"() {
    given:
    service.index(SampleData.firms()[0])
    engine.clearLog()

    when:
    def firm = service.get('firm-1', Firm)

    then:
    firm.present
    engine.log() == ['get firms/firm-1']

    when:
    service.get(null, Firm)

    then:
    thrown(NullPointerException)
  }

  def "index honours write options"() {
    given:
    Firm firm = SampleData.firms()[1]
    service.index(firm, IndexOptions.refreshed())

    when:
    service.index(firm, new IndexOptions(true, IndexOptions.Refresh.NONE))

    then:
    thrown(com.phatjam98.elasticprotos.engine.EngineException)
  }

  def "bulkIndex writes several messages, of mixed types"() {
    given:
    List<Document> documents = SampleData.documents()

    when:
    BulkResult result = service.bulkIndex(documents + [SampleData.firms()[2]])

    then:
    !result.errors()
    result.items().size() == documents.size() + 1
    documents.every { service.get(it.id, Document).get() == it.toBuilder().clearInternalNotes().build() }
    service.get('firm-3', Firm).present
    service.bulkIndex([]).items().isEmpty()
  }

  def "search compiles criteria, reads hits back and returns the next page"() {
    given:
    service.bulkIndex(SampleData.firms())

    when:
    def page = service.search(Criteria.where('status').equalTo('ACTIVE').build(), Firm)

    then:
    page.items()*.id == ['firm-1', 'firm-2']
    page.items().every { it.status == FirmStatus.ACTIVE }
    page.hits()*.id() == ['firm-1', 'firm-2']
    page.total() == 2
    page.totalExact()
    page.next().from == 2
    page.warnings().isEmpty()

    when:
    def all = service.search(Criteria.builder().pageSize(2).build(), Firm)

    then:
    all.items().size() == 2
    all.total() == 3
    all.next().from == 2
  }

  def "search returns the aggregation results with the page, empty when there are none"() {
    given:
    service.bulkIndex(SampleData.firms())

    when:
    def page = service.search(Criteria.builder().pageSize(1).aggregate(Aggs.terms('status', 'status')).build(), Firm)

    then:
    page.items().size() == 1
    page.total() == 3
    page.aggregations().buckets('status').collectEntries { [(it.key()): it.docCount()] } ==
        [ACTIVE: 2L, SUSPENDED: 1L]

    and:
    service.search(Criteria.builder().build(), Firm).aggregations() == AggregationResults.empty()
  }

  def "aggregate runs only the aggregations, without hits or highlights"() {
    given:
    service.bulkIndex(SampleData.documents())
    engine.clearLog()

    when:
    def page = service.aggregate(Criteria.where('type').equalTo('FILING').end().pageSize(50).highlight()
        .aggregate(Aggs.terms('tags', 'tags'), Aggs.sum('total', 'size_bytes')).build(), Document)
    def aggs = page.results()

    then:
    page.total() == 3
    page.warnings().isEmpty()
    aggs.buckets('tags').collectEntries { [(it.key()): it.docCount()] } ==
        [corporate: 2L, discovery: 1L, 'm-and-a': 1L]
    aggs.metric('total').asLong() == 2_870_000L
    engine.log().count { it.startsWith('search ') } == 1
  }

  def "aggregate reports compiler warnings, and no total when totals are not tracked"() {
    given:
    def lenient = new ElasticProtosService(engine, service.codec(),
        QueryCompiler.of(QueryCompilerConfig.standard().withLenient(true).withTrackTotalHits(TrackTotalHits.none())),
        null, null)
    service.bulkIndex(SampleData.documents())

    when:
    def page = lenient.aggregate(Criteria.builder().aggregate(Aggs.terms('x', 'nope'), Aggs.terms('t', 'type')).build(), Document)

    then:
    page.total() == -1
    page.warnings() == ["Skipped aggregation 'x' on unknown field 'nope'"]
    page.results().names() == ['t'] as Set
    new AggregationPage(null, 0, []).results().isEmpty()

    and: 'search logs and returns them too'
    lenient.search(Criteria.where('nope').equalTo('x').build(), Document).warnings() ==
        ["Skipped EQUALS on unknown field 'nope'"]
  }

  def "aggregate reads filter, filters and three-level aggregations"() {
    given:
    service.bulkIndex(SampleData.documents())

    when:
    def page = service.aggregate(Criteria.builder().aggregate(
        Aggs.filters('kinds', Aggs.keyed('filings', Criteria.where('type').equalTo('FILING')),
            Aggs.keyed('motions', Criteria.where('type').equalTo('MOTION'))),
        Aggs.sub(Aggs.filter('plaintiffs', 'parties', Criteria.where('parties.role').equalTo('PLAINTIFF')),
            Aggs.cardinality('docs', 'id')),
        Aggs.sub(Aggs.terms('by_type', 'type'), Aggs.sub(Aggs.terms('roles', 'parties.role'),
            Aggs.cardinality('firms', 'firm_id')))).build(), Document)

    then:
    page.results().buckets('kinds').collectEntries { [(it.key()): it.docCount()] } == [filings: 3L, motions: 1L]
    page.results().bucket('plaintiffs').sub().metric('docs').asLong() == 3L
    page.results().buckets('by_type')[0].sub().buckets('roles')*.sub()*.metric('firms')*.asLong() == [2L, 1L]
  }

  def "composite returns a pager that runs only that aggregation, page by page"() {
    given:
    com.phatjam98.elasticprotos.engine.SearchEngine fake = Mock()
    def paging = new ElasticProtosService(fake, service.codec(), QueryCompiler.withDefaults(), null, null)
    def criteria = Criteria.where('type').equalTo('FILING').end()
        .aggregate(Aggs.composite('pairs', Aggs.termsSource('firm', 'firm_id')), Aggs.terms('t', 'tags')).build()
    List<Map> bodies = []

    when:
    def pager = paging.composite(criteria, Document, 'pairs')
    def keys = []
    pager.forEachRemaining { page -> keys.addAll(page*.key()) }

    then:
    2 * fake.search(['documents'], _) >> { args ->
      bodies << args[1]
      bodies.size() == 1
          ? new com.phatjam98.elasticprotos.engine.SearchResult(3, true, [], null,
              [pairs: [after_key: [firm: 'firm-3'], buckets: [[key: [firm: 'firm-1'], doc_count: 2], [key: [firm: 'firm-3'], doc_count: 1]]]], 1)
          : new com.phatjam98.elasticprotos.engine.SearchResult(3, true, [], null, [pairs: [buckets: []]], 1)
    }
    keys == [[firm: 'firm-1'], [firm: 'firm-3']]
    bodies*.aggs*.keySet() == [['pairs'] as Set, ['pairs'] as Set]
    bodies[1].aggs.pairs.composite.after == [firm: 'firm-3']
    pager.afterKey() == Optional.of([firm: 'firm-3'])

    when: 'the in-memory engine refuses composite aggregations, like other unsupported kinds'
    service.composite(criteria, Document, 'pairs').hasNext()

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.BAD_REQUEST

    when:
    service.composite(criteria, Document, 't')

    then:
    thrown(IllegalArgumentException)
  }

  def "search asks for nested highlights through inner hits and returns them on the hits"() {
    given:
    com.phatjam98.elasticprotos.engine.SearchEngine fake = Mock()
    def searching = new ElasticProtosService(fake, service.codec(), QueryCompiler.withDefaults(), null, null)
    def doc = SampleData.documents()[0]
    def nested = com.phatjam98.elasticprotos.engine.NestedHighlights.of([new com.phatjam98.elasticprotos.engine.NestedHighlight(
        [new com.phatjam98.elasticprotos.engine.NestedLevel('parties', 0)], ['parties.name': ['<em>Acme</em> Corp']])],
        [new com.phatjam98.elasticprotos.engine.NestedTotal([], 'parties', 1, 1, false)])
    Map body = null

    when:
    def page = searching.search(Criteria.where('parties.name').matches('acme').end().highlight('parties.name').build(), Document)

    then:
    1 * fake.search(['documents'], _) >> { args ->
      body = args[1]
      new com.phatjam98.elasticprotos.engine.SearchResult(1, true, [new com.phatjam98.elasticprotos.engine.Hit(
          'documents-v1', doc.id, 1.0d, service.codec().toDocument(doc), [], null, null, null, [:], nested)], null, [:], 1)
    }
    body.query.bool.must[0].nested.inner_hits.name == 'parties__highlight'
    !body.containsKey('highlight')
    page.items() == [doc]
    page.hits()[0].nestedHighlights() == nested
    page.hits()[0].fragments('parties.name') == ['<em>Acme</em> Corp']
  }

  def "the six-component SearchPage constructor supplies empty aggregations"() {
    expect:
    new SearchPage([], [], 0, true, null, []).aggregations() == AggregationResults.empty()
    new SearchPage([], [], 0, true, null, [], null).aggregations().isEmpty()
  }

  def "search rejects criteria that do not fit the index"() {
    when:
    service.search(Criteria.where('no_such_field').equalTo('x').build(), Firm)

    then:
    thrown(QueryCompileException)
  }

  def "pager walks every page with a point in time"() {
    given:
    service.bulkIndex(SampleData.documents())

    when:
    def pager = service.pager(Criteria.builder().build(), Document, 4)
    def ids = []
    pager.each { result -> ids.addAll(result.hits()*.id()) }
    pager.close()

    then:
    ids as Set == SampleData.documents()*.id as Set
  }

  def "delete removes a document"() {
    given:
    service.index(SampleData.firms()[0])

    expect:
    service.delete('firm-1', Firm)
    !service.delete('firm-1', Firm)
    service.get('firm-1', Firm).isEmpty()
  }

  def "delete passes write options to the engine"() {
    given:
    service.index(SampleData.firms()[0])
    def recorded = []
    def recording = new InMemorySearchEngine() {
      @Override
      boolean delete(String index, String id, IndexOptions options) {
        recorded << [index, id, options]
        true
      }
    }
    def withRecording = new ElasticProtosService(recording, service.codec(), null, null, null)

    expect:
    service.delete('firm-1', Firm, IndexOptions.refreshed())
    service.get('firm-1', Firm).isEmpty()
    withRecording.delete('x', Firm, IndexOptions.refreshed())
    withRecording.delete('y', Firm)
    recorded == [['firms', 'x', IndexOptions.refreshed()], ['firms', 'y', IndexOptions.defaults()]]
  }

  def "plan and bootstrap delegate to the migrator"() {
    expect:
    service.plan(Firm, Document).items()*.action() == [Action.NO_OP, Action.NO_OP]
    service.bootstrap(Firm).ok()
  }

  def "bulkBuffer is configured from elastic-protos.bulk"() {
    given:
    def buffer = service.bulkBuffer()

    when:
    service.codec()
    buffer.add(service.indexOp(SampleData.firms()[0]))

    then:
    buffer.pending() == 1

    when:
    buffer.add(service.indexOp(SampleData.firms()[1]))

    then: 'two actions fill a batch'
    buffer.pending() == 0
    service.get('firm-2', Firm).present

    when:
    buffer.add(service.indexOp(SampleData.firms()[2]))
    buffer.close()

    then:
    service.get('firm-3', Firm).present
  }

  def "bulkBuffer reports every flushed batch to a listener"() {
    given:
    List<BulkResult> results = []
    def buffer = service.bulkBuffer { BulkResult result -> results << result }

    when: 'two actions fill a batch'
    buffer.add(service.indexOp(SampleData.firms()[0]))
    buffer.add(service.indexOp(SampleData.firms()[1]))

    then:
    results.size() == 1
    !results[0].errors()
    results[0].items()*.id() == ['firm-1', 'firm-2']

    when:
    buffer.add(service.indexOp(SampleData.firms()[2]))
    buffer.close()

    then:
    results.size() == 2
    results[1].items()*.id() == ['firm-3']
  }

  def "bulkBuffer hands failed batches and their errors to an error listener"() {
    given:
    List<BulkResult> results = []
    List<List<BulkOp>> batches = []
    List<Throwable> errors = []
    String index = service.definition(Firm).name()
    engine.setWriteBlock(index, true)
    def buffer = service.bulkBuffer({ BulkResult result -> results << result },
        { List<BulkOp> batch, Throwable error -> batches << batch; errors << error } as BiConsumer)
    def first = service.indexOp(SampleData.firms()[0])
    def second = service.indexOp(SampleData.firms()[1])

    when:
    buffer.add(first)
    buffer.add(second)

    then:
    results.isEmpty()
    batches == [[first, second]]
    errors.size() == 1
    errors[0] instanceof EngineException

    cleanup:
    engine.setWriteBlock(index, false)
    buffer?.close()
  }

  def "bulkBuffer close throws when the final batch fails"() {
    given:
    List<List<BulkOp>> batches = []
    String index = service.definition(Firm).name()
    def buffer = service.bulkBuffer({ BulkResult result -> },
        { List<BulkOp> batch, Throwable error -> batches << batch } as BiConsumer)
    def op = service.indexOp(SampleData.firms()[0])
    buffer.add(op)
    engine.setWriteBlock(index, true)

    when:
    buffer.close()

    then:
    thrown(EngineException)
    batches == [[op]]

    cleanup:
    engine.setWriteBlock(index, false)
  }

  def "the deprecated bulkBuffer overload still reports the exception"() {
    given:
    List<Throwable> errors = []
    String index = service.definition(Firm).name()
    engine.setWriteBlock(index, true)
    def buffer = service.bulkBuffer({ BulkResult result -> },
        { Throwable error -> errors << error } as Consumer)

    when:
    buffer.add(service.indexOp(SampleData.firms()[0]))
    buffer.add(service.indexOp(SampleData.firms()[1]))

    then:
    errors.size() == 1
    errors[0] instanceof EngineException

    cleanup:
    engine.setWriteBlock(index, false)
    buffer?.close()
  }
}
