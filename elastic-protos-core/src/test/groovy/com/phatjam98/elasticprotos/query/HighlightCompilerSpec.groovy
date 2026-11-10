package com.phatjam98.elasticprotos.query

import com.phatjam98.elasticprotos.mapping.IndexDefinition
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.Highlight
import com.phatjam98.elasticprotos.proto.HighlightField
import com.phatjam98.elasticprotos.proto.SearchCriteria
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.TypedMaps
import com.phatjam98.elasticprotos.proto.example.Binder
import com.phatjam98.elasticprotos.proto.test.Folders
import com.phatjam98.elasticprotos.proto.test.Geo
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class HighlightCompilerSpec extends Specification {

  @Shared ProtoMapper mapper = ProtoMapper.withDefaults()
  @Shared IndexDefinition document = mapper.map(Document)
  @Shared IndexDefinition geo = mapper.map(Geo)
  @Shared IndexDefinition typedMaps = mapper.map(TypedMaps)
  @Shared IndexDefinition binder = mapper.map(Binder)
  @Shared IndexDefinition folders = mapper.map(Folders)
  @Shared QueryCompiler strict = QueryCompiler.withDefaults()
  @Shared QueryCompiler lenient = QueryCompiler.of(QueryCompilerConfig.standard().withLenient(true))

  static SearchCriteria highlight(Highlight.Builder highlight) {
    SearchCriteria.newBuilder().setHighlight(highlight).build()
  }

  static HighlightField field(String name) {
    HighlightField.newBuilder().setField(name).build()
  }

  def "no highlight leaves it out of the body"() {
    expect:
    !strict.compile(SearchCriteria.defaultInstance, document).body().containsKey('highlight')
  }

  def "by default every full-text text field outside nested objects is highlighted"() {
    expect: 'parties.name is nested and left out'
    strict.compile(highlight(Highlight.newBuilder()), document).body().highlight ==
        [fields: [body: [:], title: [:]], pre_tags: ['<em>'], post_tags: ['</em>']]

    and: 'a typed map of text is highlighted by its pattern'
    strict.compile(highlight(Highlight.newBuilder()), typedMaps).body().highlight.fields == ['notes.*': [:]]
  }

  def "explicit fields take text fields, typed map keys and patterns"() {
    expect:
    strict.compile(highlight(Highlight.newBuilder().addFields(field('notes.summary')).addFields(field('notes.*'))),
        typedMaps).body().highlight.fields == ['notes.*': [:], 'notes.summary': [:]]
  }

  def "an index without highlightable fields warns and adds no clause"() {
    when:
    def compiled = strict.compile(highlight(Highlight.newBuilder()), geo)

    then:
    !compiled.body().containsKey('highlight')
    compiled.warnings() == ['Skipped highlight: no text field outside nested objects to highlight']
  }

  @Unroll
  def "#name is refused in strict mode and skipped with a warning in lenient mode"() {
    when:
    strict.compile(highlight(Highlight.newBuilder().addFields(field(name))), document)

    then:
    def e = thrown(QueryCompileException)
    e.message.contains(message)
    e.field == name

    when:
    def compiled = lenient.compile(highlight(Highlight.newBuilder().addFields(field(name)).addFields(field('title'))), document)

    then:
    compiled.body().highlight.fields == [title: [:]]
    compiled.warnings().size() == 1
    compiled.warnings()[0].contains(message)

    where:
    name           | message
    'nope'         | "nknown field 'nope'"
    'type'         | "highlight needs a text field but 'type' is mapped as KEYWORD"
    'parties.name' | "highlight on 'parties.name' needs a full-text condition of the query on that field"
  }

  def "an unknown field in strict mode uses the shared message"() {
    when:
    strict.compile(highlight(Highlight.newBuilder().addFields(field('nope'))), document)

    then:
    def e = thrown(QueryCompileException)
    e.message == "Unknown field 'nope' in highlight; index documents does not map it"
  }

  @Unroll
  def "invalid settings are refused: #description"() {
    when:
    strict.compile(highlight(builder), document)

    then:
    def e = thrown(QueryCompileException)
    e.message.contains(message)

    where:
    description              | builder                                                                                       || message
    'empty field name'       | Highlight.newBuilder().addFields(field(''))                                                   || 'needs a field name'
    'negative fragment size' | Highlight.newBuilder().setFragmentSize(-1)                                                    || 'highlight.fragment_size must not be negative'
    'negative fragments'     | Highlight.newBuilder().setNumberOfFragments(-1)                                               || 'number_of_fragments must not be negative'
    'field fragment size'    | Highlight.newBuilder().addFields(HighlightField.newBuilder().setField('title').setFragmentSize(-2)) || "fragment_size of highlight field 'title'"
    'field fragments'        | Highlight.newBuilder().addFields(HighlightField.newBuilder().setField('title').setNumberOfFragments(-2)) || "number_of_fragments of highlight field 'title'"
  }

  // ---- nested fields -------------------------------------------------------------------------

  static Map innerHits(Map query) {
    Map found = [:]
    def walk
    walk = { node ->
      if (node instanceof Map) {
        if (node.nested?.inner_hits) {
          found[node.nested.inner_hits.name] = node.nested.inner_hits + [path: node.nested.path]
        }
        node.values().each { walk(it) }
      } else if (node instanceof List) {
        node.each { walk(it) }
      }
    }
    walk(query)
    found
  }

  static final Map TAGS = [pre_tags: ['<em>'], post_tags: ['</em>']]

  def "a nested field is highlighted through inner_hits on the nested clause of its path, with its query as highlight query"() {
    when:
    def body = strict.compile(Criteria.where('parties.name').matches('acme').end().highlight('title', 'parties.name').build(),
        document).body()

    then: 'root fields stay in highlight'
    body.highlight == [fields: [title: [:]]] + TAGS

    and:
    body.query == [bool: [must: [[nested: [path: 'parties', query: [match: ['parties.name': 'acme']],
        inner_hits: [name: 'parties__highlight', _source: false,
                     highlight: [fields: ['parties.name': [highlight_query: [match: ['parties.name': 'acme']]]]] + TAGS]]]]]]
  }

  def "only nested fields leave the root highlight out; every setting goes into the inner hits"() {
    when:
    def compiled = strict.compile(Criteria.where('parties.name').matches('acme').end().highlight {
      it.addFields(HighlightField.newBuilder().setField('parties.name').setNumberOfFragments(0))
          .addPreTags('<b>').addPostTags('</b>').setFragmentSize(50).setNumberOfFragments(2)
          .setRequireFieldMatch(false).setHtmlEncode(true).setNestedObjects(10)
    }.build(), document)

    then:
    !compiled.body().containsKey('highlight')
    compiled.warnings().isEmpty()
    innerHits(compiled.body().query) == [parties__highlight: [name: 'parties__highlight', _source: false, size: 10,
        path: 'parties', highlight: [fields: ['parties.name': [number_of_fragments: 0,
                                                               highlight_query: [match: ['parties.name': 'acme']]]],
                                     pre_tags: ['<b>'], post_tags: ['</b>'], fragment_size: 50, number_of_fragments: 2,
                                     require_field_match: false, encoder: 'html']]]
  }

  def "several clauses on one path get unique names and one highlight query ORing all of them"() {
    when: 'two branches, each with a clause on parties'
    def two = strict.compile(Criteria.where('parties.name').matches('acme').or().where('parties.name').matches('globex')
        .end().highlight('parties.name').build(), document).body()
    def hits = innerHits(two.query)

    then:
    hits.keySet() == ['parties__highlight', 'parties__highlight2'] as Set
    hits.values()*.highlight*.fields*.get('parties.name')*.highlight_query as Set ==
        [[bool: [should: [[match: ['parties.name': 'acme']], [match: ['parties.name': 'globex']]]]]] as Set

    when: 'one branch whose clauses on parties are grouped into one nested query'
    def grouped = strict.compile(Criteria.where('parties.name').matches('acme').and('parties.role').equalTo('PLAINTIFF')
        .end().highlight('parties.name').build(), document).body()

    then:
    innerHits(grouped.query).keySet() == ['parties__highlight'] as Set
  }

  def "a nested field is refused, or skipped when lenient, when no positive clause on its path can mark it up"() {
    when:
    strict.compile(criteria.highlight('parties.name').build(), document)

    then:
    def e = thrown(QueryCompileException)
    e.message.startsWith("highlight on 'parties.name' needs a full-text condition of the query on that field (TEXT_MATCHES, TEXT_CONTAINS or FULL_TEXT) inside nested path 'parties'")
    e.message.endsWith(reason)
    e.field == 'parties.name'

    when:
    def compiled = lenient.compile(criteria.highlight('parties.name').build(), document)

    then:
    compiled.warnings().size() == 2
    compiled.warnings()[0].startsWith("Skipped highlight on 'parties.name' needs a full-text condition")
    compiled.warnings()[1] == 'Skipped highlight: no field to highlight; fields inside nested objects need a full-text condition of the query on them inside their nested path'
    !compiled.body().containsKey('highlight')
    innerHits(compiled.body().query).isEmpty()

    where:
    criteria                                                          || reason
    Criteria.where('title').matches('acme').end()                     || "and the query has none on 'parties'"
    Criteria.where('parties.role').not().equalTo('COUNSEL').end()     || "and the query has none on 'parties'"
    Criteria.where('parties.role').equalTo('PLAINTIFF').end()         || "and the conditions on 'parties' cannot mark up 'parties.name'"
    Criteria.where('parties').exists().end()                          || "and the conditions on 'parties' cannot mark up 'parties.name'"
    Criteria.where('parties.name').equalTo('Acme Corp').end()         || "and the conditions on 'parties' cannot mark up 'parties.name'"
  }

  def "with require_field_match false a full-text condition on another field of the path is enough"() {
    when:
    def compiled = strict.compile(Criteria.where('sections.heading').matches('red').end().highlight {
      it.addFields(field('sections.heading')).setRequireFieldMatch(false)
    }.build(), binder)

    then:
    innerHits(compiled.body().query).keySet() == ['sections__highlight'] as Set

    when: 'not with keyword conditions only'
    lenient.compile(Criteria.where('parties.role').equalTo('X').end().highlight {
      it.addFields(field('parties.name')).setRequireFieldMatch(false)
    }.build(), document).warnings()[0].contains("needs a full-text condition of the query (TEXT_MATCHES")

    then:
    noExceptionThrown()
  }

  def "two nested levels: the outer clause carries inner hits so that the inner ones are returned"() {
    when:
    def body = strict.compile(Criteria.where('sections.notes.text').matches('red').and('sections.heading').matches('red')
        .end().highlight('sections.heading', 'sections.notes.text', 'title').build(), binder).body()
    def hits = innerHits(body.query)

    then: 'the heading clause and the notes clause are separate nested queries on sections'
    hits.keySet() == ['sections__highlight', 'sections__highlight2', 'sections.notes__highlight'] as Set
    hits['sections.notes__highlight'].highlight.fields ==
        ['sections.notes.text': [highlight_query: [match: ['sections.notes.text': 'red']]]]
    hits['sections.notes__highlight'].path == 'sections.notes'
    [hits.sections__highlight, hits.sections__highlight2]*.highlight*.fields*.keySet() ==
        [['sections.heading'] as Set, ['sections.heading'] as Set]

    and: 'title is not queried but stays in the root highlight, as before'
    body.highlight.fields == [title: [:]]
  }

  def "an outer clause without fields of its own carries the inner hits without highlight, with the same size"() {
    when:
    def body = strict.compile(Criteria.where('sections.notes.text').matches('red').end().highlight {
      it.addFields(field('sections.notes.text')).setNestedObjects(2)
    }.build(), binder).body()

    then:
    body.query == [bool: [must: [[nested: [path: 'sections',
        inner_hits: [name: 'sections__highlight', _source: false, size: 2],
        query: [nested: [path: 'sections.notes', query: [match: ['sections.notes.text': 'red']],
            inner_hits: [name: 'sections.notes__highlight', _source: false, size: 2,
                         highlight: [fields: ['sections.notes.text': [highlight_query: [match: ['sections.notes.text': 'red']]]]] + TAGS]]]]]]]]
  }

  def "a nested path under a plain object of a nested object"() {
    when:
    def body = strict.compile(Criteria.where('sections.meta.annotations.text').matches('ink').end()
        .highlight('sections.meta.annotations.text').build(), binder).body()

    then:
    innerHits(body.query).collectEntries { [(it.key): it.value.path] } ==
        [sections__highlight: 'sections', 'sections.meta.annotations__highlight': 'sections.meta.annotations']
  }

  def "include_nested adds the nested full-text fields to the defaults, for the paths that can mark them up"() {
    when:
    def compiled = strict.compile(Criteria.builder().fullText('acme').highlight { it.setIncludeNested(true) }.build(), document)

    then:
    compiled.body().highlight.fields == [body: [:], title: [:]]
    innerHits(compiled.body().query).collectEntries { [(it.key): it.value.highlight.fields.keySet()] } ==
        [parties__highlight: ['parties.name'] as Set]

    when: 'no clause on parties, or only keyword ones: skipped silently'
    def quiet = strict.compile(criteria.highlight { it.setIncludeNested(true) }.build(), document)

    then:
    quiet.warnings().isEmpty()
    quiet.body().highlight.fields == [body: [:], title: [:]]
    innerHits(quiet.body().query).isEmpty()

    when: 'without include_nested the defaults stay outside nested objects'
    def plain = strict.compile(Criteria.builder().fullText('acme').highlight().build(), document)

    then:
    innerHits(plain.body().query).isEmpty()

    where:
    criteria << [Criteria.where('title').matches('acme').end(), Criteria.where('parties.role').equalTo('X').end()]
  }

  def "nested_objects must be between 0 and maxInnerResultWindow, checked before anything else"() {
    when:
    compiler.compile(Criteria.where('title').matches('a').end().highlight { it.addFields(field('nope')).setNestedObjects(value) }.build(), document)

    then:
    def e = thrown(QueryCompileException)
    e.message == message

    where:
    compiler | value || message
    strict   | -1    || 'highlight.nested_objects must not be negative: -1'
    lenient  | 101   || 'highlight.nested_objects = 101 exceeds the inner hits window of 100 (QueryCompilerConfig.maxInnerResultWindow, the default index.max_inner_result_window); raise both to allow more'
  }

  def "a raised maxInnerResultWindow allows more nested objects"() {
    expect:
    innerHits(QueryCompiler.of(QueryCompilerConfig.standard().withMaxInnerResultWindow(500))
        .compile(Criteria.where('parties.name').matches('a').end().highlight { it.addFields(field('parties.name')).setNestedObjects(500) }
            .build(), document).body().query).parties__highlight.size == 500
  }

  def "a search without hits compiles no highlight, so a nested field without a clause does not throw"() {
    when:
    def body = strict.compile(Criteria.where('title').matches('x').end().highlight('parties.name', 'title').aggregationsOnly().build(),
        document).body()

    then:
    body.size == 0
    !body.containsKey('highlight')
    body.query == [bool: [must: [[match: [title: 'x']]]]]
  }

  def "compileQuery is untouched by highlighting"() {
    expect:
    innerHits(strict.compileQuery(Criteria.where('parties.name').matches('a').end().highlight('parties.name').build(), document)).isEmpty()
  }

  def "typed map nested values: a concrete key is highlighted, a pattern is refused"() {
    when:
    def body = strict.compile(Criteria.where('folders.work.pages.text').matches('red').end()
        .highlight('folders.work.pages.text').build(), folders).body()

    then:
    innerHits(body.query).collectEntries { [(it.key): it.value.path] } ==
        ['folders.work.pages__highlight': 'folders.work.pages']

    when:
    strict.compile(Criteria.where('folders.work.pages.text').matches('red').end().highlight('folders.*.pages.text').build(), folders)

    then:
    def e = thrown(QueryCompileException)
    e.message == "highlight cannot use pattern 'folders.*.pages.text' inside the nested values 'folders.*.pages' of a typed map: their nested path differs per key; name a key, such as 'folders.<key>.pages.text', and query that path"

    expect: 'the default fields leave such patterns out'
    strict.compile(Criteria.builder().highlight { it.setIncludeNested(true) }.build(), folders).warnings() ==
        ['Skipped highlight: no text field outside nested objects to highlight']
  }

  def "every setting is emitted when set"() {
    expect:
    strict.compile(highlight(Highlight.newBuilder()
        .addFields(HighlightField.newBuilder().setField('title').setFragmentSize(50).setNumberOfFragments(0))
        .setFragmentSize(120).setNumberOfFragments(2).addPreTags('<mark>').addPostTags('</mark>')
        .setRequireFieldMatch(true).setHtmlEncode(true)), document).body().highlight ==
        [fields: [title: [fragment_size: 50, number_of_fragments: 0]], pre_tags: ['<mark>'], post_tags: ['</mark>'],
         fragment_size: 120, number_of_fragments: 2, require_field_match: true, encoder: 'html']
  }
}
