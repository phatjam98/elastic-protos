package com.acme.search

import com.acme.search.proto.Document
import com.acme.search.proto.DocumentType
import com.acme.search.proto.PartyRole
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.InvalidProtocolBufferException
import spock.lang.Specification
import spock.lang.Subject

/** Docker-free: protobuf JSON in and out of {@link DocumentJson}. */
class DocumentJsonSpec extends Specification {

  @Subject
  DocumentJson json = new DocumentJson(new ObjectMapper())

  def 'a document round-trips through protobuf JSON'() {
    given:
    Document original = SampleSeeder.samples()[0]

    when:
    Document parsed = json.parse(json.print(original))

    then:
    parsed == original
  }

  def 'parses snake_case and camelCase names, enums by name and RFC 3339 timestamps'() {
    when:
    Document doc = json.parse('''{
      "id": "doc-9",
      "firm_id": "firm-1",
      "title": "Merger agreement",
      "type": "MOTION",
      "parties": [{"name": "Acme Corp", "role": "PLAINTIFF"}],
      "filedAt": "2026-03-02T15:04:05Z"
    }''')

    then:
    doc.id == 'doc-9'
    doc.firmId == 'firm-1'
    doc.type == DocumentType.MOTION
    doc.getParties(0).role == PartyRole.PLAINTIFF
    doc.filedAt.seconds == 1772463845L
  }

  def 'the JSON tree uses protobuf JSON names'() {
    when:
    def tree = json.toTree(SampleSeeder.samples()[0])

    then:
    tree.get('firmId').asText() == 'firm-1'
    tree.get('type').asText() == 'FILING'
    tree.get('parties').size() == 2
  }

  def 'parses an array of documents'() {
    expect:
    json.parseArray('[{"id": "a"}, {"id": "b"}]')*.id == ['a', 'b']
  }

  def 'rejects JSON that does not fit the message'() {
    when:
    json.parse('{"no_such_field": 1}')

    then:
    thrown(InvalidProtocolBufferException)
  }

  def 'rejects a bulk body that is not an array'() {
    when:
    json.parseArray(input)

    then:
    thrown(InvalidProtocolBufferException)

    where:
    input << ['{"id": "a"}', 'not json']
  }
}
