package com.phatjam98.elasticprotos.query

import com.phatjam98.elasticprotos.mapping.IndexDefinition
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.Highlight
import com.phatjam98.elasticprotos.proto.HighlightField
import com.phatjam98.elasticprotos.proto.SearchCriteria
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.TypedMaps
import com.phatjam98.elasticprotos.proto.test.Archive
import com.phatjam98.elasticprotos.proto.test.Geo
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class HighlightCompilerSpec extends Specification {

  @Shared ProtoMapper mapper = ProtoMapper.withDefaults()
  @Shared IndexDefinition document = mapper.map(Document)
  @Shared IndexDefinition geo = mapper.map(Geo)
  @Shared IndexDefinition typedMaps = mapper.map(TypedMaps)
  @Shared IndexDefinition archive = mapper.map(Archive)
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
    'parties.name' | "highlight on 'parties.name' needs a condition of the query on nested path 'parties'"
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

  def "a nested field is highlighted through inner_hits on the nested clause of its path"() {
    when:
    def body = strict.compile(Criteria.where('parties.name').matches('acme').end().highlight('title', 'parties.name').build(),
        document).body()

    then: 'root fields stay in highlight'
    body.highlight == [fields: [title: [:]], pre_tags: ['<em>'], post_tags: ['</em>']]

    and:
    body.query == [bool: [must: [[nested: [path: 'parties', query: [match: ['parties.name': 'acme']],
        inner_hits: [name: 'parties__highlight', _source: false,
                     highlight: [fields: ['parties.name': [:]], pre_tags: ['<em>'], post_tags: ['</em>']]]]]]]]
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
        path: 'parties', highlight: [fields: ['parties.name': [number_of_fragments: 0]], pre_tags: ['<b>'],
                                     post_tags: ['</b>'], fragment_size: 50, number_of_fragments: 2,
                                     require_field_match: false, encoder: 'html']]]
  }

  def "several clauses on one path get inner hits with unique names; grouped clauses get one"() {
    when: 'two branches, each with a clause on parties'
    def two = strict.compile(Criteria.where('parties.name').matches('acme').or().where('parties.name').matches('globex')
        .end().highlight('parties.name').build(), document).body()

    then:
    innerHits(two.query).keySet() == ['parties__highlight', 'parties__highlight2'] as Set

    when: 'one branch whose clauses on parties are grouped into one nested query'
    def grouped = strict.compile(Criteria.where('parties.name').matches('acme').and('parties.role').equalTo('PLAINTIFF')
        .end().highlight('parties.name').build(), document).body()

    then:
    innerHits(grouped.query).keySet() == ['parties__highlight'] as Set
  }

  def "a nested field without a positive clause on its path is refused, or skipped when lenient"() {
    when:
    strict.compile(criteria.highlight('parties.name').build(), document)

    then:
    def e = thrown(QueryCompileException)
    e.message == "highlight on 'parties.name' needs a condition of the query on nested path 'parties': the engines highlight nested fields only within the inner hits of a nested query on that path"
    e.field == 'parties.name'

    when:
    def compiled = lenient.compile(criteria.highlight('parties.name').build(), document)

    then:
    compiled.warnings() == ["Skipped highlight on 'parties.name' needs a condition of the query on nested path 'parties': the engines highlight nested fields only within the inner hits of a nested query on that path",
                            'Skipped highlight: no field to highlight; fields inside nested objects need a condition of the query on their nested path']
    !compiled.body().containsKey('highlight')
    innerHits(compiled.body().query).isEmpty()

    where:
    criteria << [Criteria.where('title').matches('acme').end(),
                 Criteria.where('parties.role').not().equalTo('COUNSEL').end()]
  }

  def "two nested levels: the outer clause carries inner hits so that the inner ones are returned"() {
    when:
    def body = strict.compile(Criteria.where('boxes.notes.text').matches('red').and('boxes.label').matches('red')
        .end().highlight('boxes.label', 'boxes.notes.text', 'title').build(), archive).body()
    def hits = innerHits(body.query)

    then:
    hits.keySet() == ['boxes__highlight', 'boxes__highlight2', 'boxes.notes__highlight'] as Set
    hits['boxes.notes__highlight'].highlight.fields == ['boxes.notes.text': [:]]
    hits['boxes.notes__highlight'].path == 'boxes.notes'
    [hits.boxes__highlight, hits.boxes__highlight2]*.highlight*.fields == [['boxes.label': [:]], ['boxes.label': [:]]]
    body.highlight.fields == [title: [:]]
  }

  def "an outer clause without fields of its own carries the inner hits without highlight"() {
    when:
    def body = strict.compile(Criteria.where('boxes.notes.text').matches('red').end().highlight('boxes.notes.text').build(),
        archive).body()

    then:
    body.query == [bool: [must: [[nested: [path: 'boxes',
        inner_hits: [name: 'boxes__highlight', _source: false],
        query: [nested: [path: 'boxes.notes', query: [match: ['boxes.notes.text': 'red']],
            inner_hits: [name: 'boxes.notes__highlight', _source: false,
                         highlight: [fields: ['boxes.notes.text': [:]], pre_tags: ['<em>'], post_tags: ['</em>']]]]]]]]]]
  }

  def "include_nested adds the nested full-text fields to the defaults, for the paths the query has a clause on"() {
    when:
    def compiled = strict.compile(Criteria.builder().fullText('acme').highlight { it.setIncludeNested(true) }.build(), document)

    then:
    compiled.body().highlight.fields == [body: [:], title: [:]]
    innerHits(compiled.body().query).collectEntries { [(it.key): it.value.highlight.fields] } ==
        [parties__highlight: ['parties.name': [:]]]

    when: 'no clause on parties: skipped silently'
    def quiet = strict.compile(Criteria.where('title').matches('acme').end().highlight { it.setIncludeNested(true) }.build(), document)

    then:
    quiet.warnings().isEmpty()
    quiet.body().highlight.fields == [body: [:], title: [:]]
    innerHits(quiet.body().query).isEmpty()

    when: 'without include_nested the defaults stay outside nested objects'
    def plain = strict.compile(Criteria.builder().fullText('acme').highlight().build(), document)

    then:
    innerHits(plain.body().query).isEmpty()
  }

  def "nested_objects must not be negative, and compileQuery is untouched by highlighting"() {
    when:
    strict.compile(Criteria.where('parties.name').matches('a').end().highlight { it.addFields(field('parties.name')).setNestedObjects(-1) }.build(), document)

    then:
    def e = thrown(QueryCompileException)
    e.message == 'highlight.nested_objects must not be negative: -1'

    expect:
    innerHits(strict.compileQuery(Criteria.where('parties.name').matches('a').end().highlight('parties.name').build(), document)).isEmpty()
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
