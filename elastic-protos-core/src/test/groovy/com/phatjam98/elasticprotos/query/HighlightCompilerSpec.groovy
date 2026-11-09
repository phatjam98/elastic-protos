package com.phatjam98.elasticprotos.query

import com.phatjam98.elasticprotos.mapping.IndexDefinition
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.Highlight
import com.phatjam98.elasticprotos.proto.HighlightField
import com.phatjam98.elasticprotos.proto.SearchCriteria
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.TypedMaps
import com.phatjam98.elasticprotos.proto.test.Geo
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class HighlightCompilerSpec extends Specification {

  @Shared ProtoMapper mapper = ProtoMapper.withDefaults()
  @Shared IndexDefinition document = mapper.map(Document)
  @Shared IndexDefinition geo = mapper.map(Geo)
  @Shared IndexDefinition typedMaps = mapper.map(TypedMaps)
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
    'parties.name' | "inside nested path 'parties'; fragments of nested fields need inner_hits"
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

  def "every setting is emitted when set"() {
    expect:
    strict.compile(highlight(Highlight.newBuilder()
        .addFields(HighlightField.newBuilder().setField('title').setFragmentSize(50).setNumberOfFragments(0))
        .setFragmentSize(120).setNumberOfFragments(2).setPreTag('<mark>').setPostTag('</mark>')
        .setRequireFieldMatch(true).setHtmlEncode(true)), document).body().highlight ==
        [fields: [title: [fragment_size: 50, number_of_fragments: 0]], pre_tags: ['<mark>'], post_tags: ['</mark>'],
         fragment_size: 120, number_of_fragments: 2, require_field_match: true, encoder: 'html']
  }
}
