package com.phatjam98.elasticprotos.query

import static com.phatjam98.elasticprotos.proto.SearchOperationType.EQUALS
import static com.phatjam98.elasticprotos.query.Q.*

import com.phatjam98.elasticprotos.engine.EngineInfo
import com.phatjam98.elasticprotos.engine.EngineProduct
import com.phatjam98.elasticprotos.engine.Hit
import com.phatjam98.elasticprotos.engine.Pit
import com.phatjam98.elasticprotos.engine.SearchEngine
import com.phatjam98.elasticprotos.engine.SearchResult
import com.phatjam98.elasticprotos.mapping.IndexDefinition
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.SearchCriteria
import com.phatjam98.elasticprotos.proto.example.Document
import java.time.Duration
import spock.lang.Shared
import spock.lang.Specification

class SearchPagerSpec extends Specification {

  @Shared IndexDefinition document = ProtoMapper.withDefaults().map(Document)
  QueryCompiler compiler = QueryCompiler.withDefaults()
  SearchCriteria criteria = where(fc('type', EQUALS, str('FILING')))

  static final EngineInfo ES = new EngineInfo(EngineProduct.ELASTICSEARCH, '9.1.0', 'es')
  static final EngineInfo OS2 = new EngineInfo(EngineProduct.OPENSEARCH, '2.19.0', 'os')

  static SearchResult page(String pitId, int count, int offset) {
    new SearchResult(100, true, (0..<count).collect {
      new Hit('documents-v1', "d${offset + it}", null, [:], [(long) (offset + it), "d${offset + it}"])
    }, pitId, [:], 1)
  }

  def "pages through three full pages and stops at the empty page"() {
    given:
    SearchEngine engine = Mock()
    List<Map> bodies = []

    when:
    List<SearchResult> pages = []
    SearchPager pager = SearchPager.open(engine, compiler, ['documents'], criteria, document, 2)
    while (pager.hasNext()) {
      pages << pager.next()
    }

    then:
    engine.info() >> ES
    1 * engine.openPit(['documents'], Duration.ofMinutes(1)) >> new Pit('pit-a', Duration.ofMinutes(1))
    4 * engine.search(['documents'], _) >> { args ->
      bodies << args[1]
      [page('pit-b', 2, 0), page('pit-c', 2, 2), page('pit-d', 2, 4), page('pit-e', 0, 6)][bodies.size() - 1]
    }
    0 * engine.closePit(_)
    pages*.hits()*.size() == [2, 2, 2]
    !pager.hasNext()

    and: "every body carries the latest pit and the previous last sort"
    bodies*.pit == [[id: 'pit-a', keep_alive: '1m'], [id: 'pit-b', keep_alive: '1m'],
                    [id: 'pit-c', keep_alive: '1m'], [id: 'pit-d', keep_alive: '1m']]
    bodies*.search_after == [null, [1L, 'd1'], [3L, 'd3'], [5L, 'd5']]
    bodies.every { it.size == 2 && !it.containsKey('from') }
    bodies.every { it.sort == [[_shard_doc: [order: 'asc']]] }
    bodies.every { it.query == [bool: [filter: [[term: [type: 'FILING']]]]] }

    when:
    pager.close()
    pager.close()

    then:
    1 * engine.closePit(new Pit('pit-e', Duration.ofMinutes(1)))
    pager.pagination().cursor == 'pit-e'
  }

  def "aggregations go with the first page only, highlighting with every page"() {
    given:
    SearchEngine engine = Mock()
    List<Map> bodies = []
    SearchCriteria withAggs = Criteria.where('type').equalTo('FILING').end()
        .aggregate(Aggs.terms('tags', 'tags')).highlight('title').build()

    when:
    SearchPager.forEachPage(engine, compiler, ['documents'], withAggs, document, 2) {}

    then:
    engine.info() >> ES
    1 * engine.openPit(_, _) >> new Pit('pit-a', Duration.ofMinutes(1))
    3 * engine.search(['documents'], _) >> { args ->
      bodies << args[1]
      [page('pit-b', 2, 0), page('pit-c', 2, 2), page('pit-d', 1, 4)][bodies.size() - 1]
    }
    1 * engine.closePit(_)
    bodies*.containsKey('aggs') == [true, false, false]
    bodies.every { it.highlight.fields == [title: [:]] }
    bodies[0].aggs == [tags: [terms: [field: 'tags']]]
  }

  def "forEachPage stops after a short page and closes the pit"() {
    given:
    SearchEngine engine = Mock()
    List<SearchResult> pages = []

    when:
    SearchPager.forEachPage(engine, compiler, ['documents'], criteria, document, 3) { pages << it }

    then:
    engine.info() >> ES
    1 * engine.openPit(_, _) >> new Pit('pit-a', Duration.ofMinutes(1))
    2 * engine.search(_, _) >>> [page('pit-a', 3, 0), page('pit-a', 1, 3)]
    1 * engine.closePit(new Pit('pit-a', Duration.ofMinutes(1)))
    pages*.hits()*.size() == [3, 1]
  }

  def "an empty first page yields nothing and next fails"() {
    given:
    SearchEngine engine = Stub {
      info() >> ES
      openPit(_, _) >> new Pit('p', Duration.ofSeconds(30))
      search(_, _) >> page(null, 0, 0)
    }
    SearchPager pager = SearchPager.open(engine, compiler, ['documents'], criteria, document, 5)

    when:
    pager.next()

    then:
    thrown(NoSuchElementException)
    !pager.hasNext()
  }

  def "a closed pager has no more pages"() {
    given:
    SearchEngine engine = Mock()
    engine.info() >> ES
    engine.openPit(_, _) >> new Pit('p', Duration.ofMinutes(1))
    SearchPager pager = SearchPager.open(engine, compiler, ['documents'], criteria, document, 5)

    when:
    pager.close()

    then:
    !pager.hasNext()
    0 * engine.search(_, _)
  }

  def "engines without _shard_doc page without the tiebreaker"() {
    given:
    SearchEngine engine = Mock()
    List<Map> bodies = []

    when:
    SearchPager.forEachPage(engine, compiler, ['documents'], criteria, document, 3) {}

    then:
    engine.info() >> OS2
    1 * engine.openPit(_, _) >> new Pit('pit-a', Duration.ofMinutes(1))
    1 * engine.search(_, _) >> { args -> bodies << args[1]; page('pit-a', 1, 0) }
    bodies*.sort == [null]
    bodies*.pit*.id == ['pit-a']
  }

  def "the page size must be positive"() {
    when:
    SearchPager.open(Mock(SearchEngine), compiler, ['documents'], criteria, document, 0)

    then:
    thrown(IllegalArgumentException)
  }

  def "a page size above maxPageSize is refused before a point in time is opened"() {
    given:
    def engine = Mock(SearchEngine)
    def small = QueryCompiler.of(QueryCompilerConfig.standard().withDefaultSize(5).withMaxPageSize(5))

    when:
    SearchPager.open(engine, small, ['documents'], criteria, document, 6)

    then:
    def e = thrown(IllegalArgumentException)
    e.message == 'pageSize 6 exceeds the maximum page size of 5 (QueryCompilerConfig.maxPageSize)'
    0 * engine.openPit(_, _)
  }
}
