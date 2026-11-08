package com.phatjam98.elasticprotos.mapping

import com.phatjam98.elasticprotos.Golden
import com.phatjam98.elasticprotos.proto.DynamicMode
import com.phatjam98.elasticprotos.proto.EsType
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.proto.example.InboxItem
import com.phatjam98.elasticprotos.proto.test.Annotated
import com.phatjam98.elasticprotos.proto.test.Composite
import com.phatjam98.elasticprotos.proto.test.Geo
import com.phatjam98.elasticprotos.proto.test.IdByOption
import com.phatjam98.elasticprotos.proto.test.Scalars
import com.phatjam98.elasticprotos.proto.test.ScalarsReordered
import com.phatjam98.elasticprotos.proto.test.Skipped
import com.phatjam98.elasticprotos.proto.test.TreeNode
import com.phatjam98.elasticprotos.proto.test.WellKnown
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class ProtoMapperSpec extends Specification {

  static final Map DISABLED = [type: 'object', enabled: false]

  @Shared
  ProtoMapper mapper = ProtoMapper.withDefaults()

  @Unroll
  def "#name maps to its golden index definition"() {
    when:
    IndexDefinition definition = mapper.map(type)

    then:
    Golden.assertMatches("mapping/${name}", [
        alias         : definition.alias(),
        name          : definition.name(),
        settings      : definition.settings(),
        mappings      : definition.mappings(),
        nestedPaths   : definition.nestedPaths() as List,
        fullTextFields: definition.fullTextFields(),
        ignoredPaths  : definition.ignoredPaths() as List,
        idField       : definition.idField(),
        warnings      : definition.warnings(),
    ])

    where:
    name           | type
    'scalars'      | Scalars
    'well_known'   | WellKnown
    'composite'    | Composite
    'tree_node'    | TreeNode
    'geo'          | Geo
    'annotated'    | Annotated
    'id_by_option' | IdByOption
    'firm'         | Firm
    'document'     | Document
    'inbox_item'   | InboxItem
  }

  def "scalars follow the default rules"() {
    when:
    def definition = mapper.map(Scalars)
    def props = definition.mappings().properties

    then:
    props.string_value == [type: 'keyword', ignore_above: 256]
    props.enum_value == [type: 'keyword', ignore_above: 256]
    props.repeated_strings == [type: 'keyword', ignore_above: 256]
    props.int32_value.type == 'integer'
    props.sint32_value.type == 'integer'
    props.uint32_value.type == 'long'
    props.fixed32_value.type == 'long'
    props.int64_value.type == 'long'
    props.fixed64_value == [type: 'keyword', ignore_above: 256]
    props.sfixed64_value.type == 'long'
    props.uint64_value.type == 'keyword'
    props.float_value.type == 'float'
    props.double_value.type == 'double'
    props.bool_value.type == 'boolean'
    props.bytes_value.type == 'binary'
    definition.mappings().dynamic == 'strict'
    definition.fields()['repeated_strings'].repeated()
    !definition.fields()['string_value'].repeated()
    definition.warnings().any { it.startsWith('uint64_value: uint64 does not fit a long') }
    definition.warnings().any { it.startsWith('fixed64_value: fixed64 does not fit a long') }
    definition.fullTextFields().isEmpty()
    definition.idField() == null
    definition.alias() == 'scalars'
    definition.name() == "scalars-v${definition.contentHash()}"
    definition.contentHash().length() == 8
  }

  def "well-known types map to their natural engine types"() {
    when:
    def definition = mapper.map(WellKnown)
    def props = definition.mappings().properties

    then:
    props.timestamp_value == [type: 'date', format: 'strict_date_optional_time']
    props.duration_value.type == 'keyword'
    props.string_wrapper.type == 'keyword'
    props.int64_wrapper.type == 'long'
    props.bool_wrapper.type == 'boolean'
    props.double_wrapper.type == 'double'
    props.struct_value == [type: 'flattened']
    props.value_value == [type: 'flattened']
    props.list_value == [type: 'flattened']
    !props.containsKey('any_value')
    !props.containsKey('field_mask')
    !props.containsKey('empty_value')
    definition.ignoredPaths() == ['any_value', 'empty_value', 'field_mask'] as Set
  }

  def "messages, repeated messages, maps and oneofs"() {
    when:
    def definition = mapper.map(Composite)
    def props = definition.mappings().properties

    then:
    props.single_child.type == 'object'
    props.single_child.properties.name.type == 'keyword'
    props.single_child.properties.grandchild == [type: 'object', properties: [note: [type: 'keyword', ignore_above: 256]]]
    props.children.type == 'nested'
    props.children.properties.rank.type == 'integer'
    props.string_map == [type: 'object', dynamic: true]
    props.message_map == [type: 'object', dynamic: true]
    props.int_map == [type: 'object', dynamic: true]
    props.choice_string.type == 'keyword'
    props.choice_int.type == 'long'
    props.choice_child.type == 'object'
    definition.nestedPaths() == ['children'] as Set
    definition.fields()['children.grandchild.note'].kind() == FieldKind.KEYWORD
    definition.fields()['children'].repeated()
    definition.fields()['string_map'].repeated()
    definition.nestedAncestors('children.grandchild.note') == ['children']
    definition.nestedAncestors('single_child.name') == []
  }

  def "a recursive message is cut off at the first recursion"() {
    when:
    def definition = mapper.map(TreeNode)
    def props = definition.mappings().properties

    then:
    props.label.type == 'keyword'
    props.children == DISABLED
    props.parent == DISABLED
    definition.nestedPaths().isEmpty()
    definition.fields()['children'].kind() == FieldKind.OBJECT
    definition.warnings().size() == 2
    definition.warnings().every { it.contains('recursive message') }
  }

  def "nesting deeper than maxDepth is cut off"() {
    given:
    def shallow = ProtoMapper.builder().defaults(MappingDefaults.standard().withMaxDepth(1)).build()

    when:
    def definition = shallow.map(Composite)
    def props = definition.mappings().properties

    then:
    props.single_child.properties.name.type == 'keyword'
    props.single_child.properties.grandchild == DISABLED
    definition.warnings().any { it.startsWith('single_child.grandchild:') && it.contains('deeper than 1') }
  }

  def "geo forms"() {
    when:
    def props = mapper.map(Geo).mappings().properties

    then:
    props.point == [type: 'geo_point']
    props.shape == [type: 'geo_shape']
    props.wkt_point == [type: 'geo_point']
    props.wkt_shape == [type: 'geo_shape']
    props.coordinates == [type: 'geo_point']
  }

  def "field and message options are honoured"() {
    when:
    def definition = mapper.map(Annotated)
    def props = definition.mappings().properties

    then:
    definition.alias() == 'annotated'
    definition.name() == "annotated_docs-v${definition.contentHash()}"
    definition.settings() == [
        number_of_shards  : 2,
        number_of_replicas: 0,
        analysis          : [analyzer: [folding: [tokenizer: 'standard', filter: ['lowercase', 'asciifolding']]]],
    ]
    definition.mappings().dynamic == false
    definition.idField() == 'id'
    props.title == [type: 'text', analyzer: 'english', fields: [keyword: [type: 'keyword', ignore_above: 256]]]
    props.body == [type: 'text', analyzer: 'folding', search_analyzer: 'standard']
    props.code == [type: 'keyword', ignore_above: 64, normalizer: 'lowercase']
    !props.containsKey('secret')
    definition.ignoredPaths() == ['secret'] as Set
    props.unsearchable == [type: 'keyword', ignore_above: 256, index: false, doc_values: false]
    !props.containsKey('renamed')
    props.renamed_field.type == 'keyword'
    definition.fields().containsKey('renamed_field')
    props.copied.copy_to == ['all_text']
    props.birthday == [type: 'date', format: 'yyyy-MM-dd']
    props.as_long.type == 'long'
    props.forced_nested.type == 'nested'
    props.forced_object.type == 'object'
    props.flattened_map == [type: 'flattened']
    props.raw == [type: 'keyword', ignore_above: 256, eager_global_ordinals: true]
    props.ip == [type: 'ip']
    props.with_null == [type: 'keyword', ignore_above: 256, null_value: 'N/A']
    definition.nestedPaths() == ['forced_nested'] as Set
    definition.fullTextFields() == [title: 2.5f, body: 1.0f, copied: 1.0f]
    definition.fields()['title'].exactMatchPath() == 'title.keyword'
    definition.fields()['body'].exactMatchPath() == 'body'
    !definition.fields()['body'].supportsExactMatch()
  }

  def "coerce is only emitted for numeric fields"() {
    given:
    def overrides = MappingOverrides.builder()
        .forField(Scalars.descriptor, 'int32_value') { it.coerce = com.phatjam98.elasticprotos.proto.Bool3.FALSE }
        .forField(Scalars.descriptor, 'string_value') { it.coerce = com.phatjam98.elasticprotos.proto.Bool3.FALSE }
        .build()

    when:
    def props = mapper.map(Scalars.descriptor, overrides).mappings().properties

    then:
    props.int32_value == [type: 'integer', coerce: false]
    props.string_value == [type: 'keyword', ignore_above: 256]
  }

  def "the id can be chosen by the message option"() {
    expect:
    mapper.map(IdByOption).idField() == 'key'
  }

  def "an id_field that does not exist is rejected"() {
    given:
    def overrides = MappingOverrides.builder().forMessage(Scalars.descriptor) { it.idField = 'nope' }.build()

    when:
    mapper.map(Scalars.descriptor, overrides)

    then:
    def e = thrown(MappingException)
    e.message.contains("id_field 'nope'")
    e.path == Scalars.descriptor.fullName
  }

  def "a skipped message is reported and refused"() {
    expect:
    ProtoMapper.isSkipped(Skipped.descriptor)
    !ProtoMapper.isSkipped(Scalars.descriptor)

    when:
    mapper.map(Skipped)

    then:
    thrown(MappingException)
  }

  def "the example Firm maps as documented"() {
    when:
    def definition = mapper.map(Firm)
    def props = definition.mappings().properties

    then:
    definition.alias() == 'firms'
    definition.name().startsWith('firms-v')
    definition.settings() == [number_of_shards: 1]
    definition.idField() == 'id'
    props.name == [type: 'text', fields: [keyword: [type: 'keyword', ignore_above: 256]]]
    props.status.type == 'keyword'
    props.headquarters.type == 'object'
    props.headquarters.properties.location == [type: 'geo_point']
    props.offices.type == 'nested'
    props.offices.properties.address.properties.location == [type: 'geo_point']
    props.labels == [type: 'object', dynamic: true]
    props.created_at.type == 'date'
    definition.nestedPaths() == ['offices'] as Set
    definition.fullTextFields() == [name: 2.0f, 'offices.name': 1.0f]
    definition.fields()['offices.name'].exactMatchPath() == 'offices.name.keyword'
  }

  def "the example Document applies the default analyzer and ignores notes"() {
    when:
    def definition = mapper.map(Document)
    def props = definition.mappings().properties

    then:
    definition.alias() == 'documents'
    props.title.analyzer == 'english'
    props.title.fields.keyword.type == 'keyword'
    props.body == [type: 'text', analyzer: 'english']
    props.parties.type == 'nested'
    props.parties.properties.name.analyzer == 'english'
    props.tags.type == 'keyword'
    props.checksum.type == 'binary'
    props.metadata.type == 'flattened'
    props.relevance.type == 'double'
    props.size_bytes.type == 'long'
    !props.containsKey('internal_notes')
    definition.ignoredPaths() == ['internal_notes'] as Set
    definition.fullTextFields().keySet() == ['body', 'parties.name', 'title'] as Set
    definition.fullTextFields().title == 3.0f
  }

  def "the content hash depends on the mapping and nothing else"() {
    given:
    def shards = MappingOverrides.builder().setting('number_of_shards', 9).build()
    def typeChange = MappingOverrides.builder().forField(Scalars.descriptor, 'string_value') { it.type = EsType.TEXT }.build()

    expect:
    mapper.map(Scalars).contentHash() == mapper.map(Scalars).contentHash()
    mapper.map(Scalars).contentHash() == mapper.map(ScalarsReordered).contentHash()
    mapper.map(Scalars.descriptor, shards).contentHash() == mapper.map(Scalars).contentHash()
    mapper.map(Scalars.descriptor, shards).settings() == [number_of_shards: 9]
    mapper.map(Scalars.descriptor, typeChange).contentHash() != mapper.map(Scalars).contentHash()
  }

  def "_meta records the hash, the proto and the schema version"() {
    when:
    def definition = mapper.map(Scalars)

    then:
    definition.mappings()._meta == [elastic_protos: [hash: definition.contentHash(), proto: 'phatjam98.elasticprotos.test.Scalars', schema_version: 1]]
  }

  def "a naming strategy can prefix aliases and index names"() {
    given:
    def dev = ProtoMapper.builder().naming(IndexNamingStrategy.prefixed('dev')).build()

    when:
    def definition = dev.map(Firm)

    then:
    definition.alias() == 'dev-firms'
    definition.name() == "dev-firms-v${definition.contentHash()}"
    definition.contentHash() == mapper.map(Firm).contentHash()
  }

  def "overrides win over proto options and can address fields by message or by path"() {
    given:
    def overrides = MappingOverrides.builder()
        .forMessage(Document.descriptor) { it.alias = 'docs' }
        .forField(Document.descriptor, 'title') { it.type = EsType.KEYWORD }
        .forPath('parties.email') { it.type = EsType.TEXT }
        .ignore(Document.descriptor.fullName, 'body')
        .ignorePath('checksum')
        .setting('index', [refresh_interval: '30s'])
        .build()
    def configured = ProtoMapper.builder().overrides(overrides).build()

    when:
    def definition = configured.map(Document)
    def props = definition.mappings().properties

    then:
    definition.alias() == 'docs'
    props.title == [type: 'keyword', ignore_above: 256]
    props.parties.properties.email.type == 'text'
    props.parties.properties.email.analyzer == 'english'
    !props.containsKey('body')
    !props.containsKey('checksum')
    definition.ignoredPaths() == ['body', 'checksum', 'internal_notes'] as Set
    definition.settings() == [index: [refresh_interval: '30s']]
    definition.fullTextFields().keySet() == ['parties.email', 'parties.name'] as Set
  }

  def "an override turns off the id, full_text and skip options a proto turned on"() {
    given:
    def off = MappingOverrides.builder()
        .id(Annotated.descriptor, 'id', false)
        .fullText(Annotated.descriptor, 'copied', false)
        .fullText(Annotated.descriptor, 'body', false)
        .fullText(Annotated.descriptor, 'code', true)
        .skip(Skipped.descriptor, false)
        .build()
    def offMapper = ProtoMapper.builder().overrides(off).build()
    def skipScalars = ProtoMapper.builder().overrides(MappingOverrides.builder().skip(Scalars.descriptor, true).build()).build()

    when:
    def declared = mapper.map(Annotated)
    def overridden = offMapper.map(Annotated)

    then: 'the proto declares id and full_text TRUE'
    declared.idField() == 'id'
    declared.fullTextFields().keySet() == ['title', 'body', 'copied'] as Set

    and: 'FALSE in an override wins over TRUE in the proto'
    overridden.idField() == null
    overridden.fullTextFields() == [title: 2.5f, code: 1.0f]
    overridden.contentHash() == declared.contentHash()   // neither option changes the index mapping

    and: 'skip follows the override, and the static check reads only the proto'
    ProtoMapper.isSkipped(Skipped.descriptor)
    !offMapper.skips(Skipped.descriptor)
    offMapper.map(Skipped).alias() == 'skipped'
    mapper.skips(Skipped.descriptor)
    skipScalars.skips(Scalars.descriptor)
    !skipScalars.skips(Document.descriptor)

    and: 'the Bool3 helper never yields UNSET'
    MappingOverrides.bool3(true) == com.phatjam98.elasticprotos.proto.Bool3.TRUE
    MappingOverrides.bool3(false) == com.phatjam98.elasticprotos.proto.Bool3.FALSE

    when:
    skipScalars.map(Scalars)

    then:
    thrown(MappingException)
  }

  def "by-message and by-path overrides merge with the path winning"() {
    given:
    def overrides = MappingOverrides.builder()
        .forField(Document.descriptor, 'title') { it.type = EsType.TEXT; it.analyzer = 'standard' }
        .forPath('title') { it.analyzer = 'simple' }
        .build()

    when:
    def props = mapper.map(Document.descriptor, overrides).mappings().properties

    then:
    props.title.analyzer == 'simple'
    props.title.type == 'text'
  }

  @Unroll
  def "an incompatible explicit type is rejected: #type on #field"() {
    given:
    def overrides = MappingOverrides.builder().forField(message.descriptor, field) { it.type = type }.build()

    when:
    mapper.map(message.descriptor, overrides)

    then:
    def e = thrown(MappingException)
    e.path == field

    where:
    message   | field          | type
    Scalars   | 'string_value' | EsType.NESTED
    Scalars   | 'int32_value'  | EsType.GEO_POINT
    Scalars   | 'bool_value'   | EsType.GEO_SHAPE
    Composite | 'single_child' | EsType.TEXT
    Composite | 'single_child' | EsType.DATE
    Scalars   | 'bytes_value'  | EsType.FLATTENED
  }

  def "invalid JSON in an option is reported with its path"() {
    given:
    def overrides = MappingOverrides.builder().forField(Scalars.descriptor, 'string_value') { it.rawMappingJson = '{nope' }.build()

    when:
    mapper.map(Scalars.descriptor, overrides)

    then:
    def e = thrown(MappingException)
    e.path == 'string_value'
    e.message.contains('invalid JSON')
  }

  def "defaults can be changed"() {
    given:
    def defaults = MappingDefaults.standard()
        .withStringsAsText(true)
        .withKeywordSubfield(false)
        .withSingularMessage(FieldKind.NESTED)
        .withRepeatedMessage(FieldKind.OBJECT)
        .withStructKind(FieldKind.OBJECT)
        .withDynamic(DynamicMode.DYNAMIC_TRUE)
        .withIgnoreAbove(32)
        .withDateFormat('epoch_millis')
    def custom = ProtoMapper.builder().defaults(defaults).build()

    when:
    def composite = custom.map(Composite).mappings()
    def wellKnown = custom.map(WellKnown).mappings().properties
    def scalars = custom.map(Scalars).mappings().properties

    then:
    custom.defaults() == defaults
    scalars.string_value == [type: 'text']
    scalars.enum_value == [type: 'keyword', ignore_above: 32]
    composite.dynamic == true
    composite.properties.single_child.type == 'nested'
    composite.properties.children.type == 'object'
    wellKnown.struct_value == [type: 'object', dynamic: true]
    wellKnown.timestamp_value == [type: 'date', format: 'epoch_millis']
  }

  def "mapping by class and by descriptor agree"() {
    expect:
    mapper.map(Firm) == mapper.map(Firm.descriptor)
    ProtoMapper.descriptorOf(Firm) == Firm.descriptor
  }

  def "descriptorOf rejects classes without a descriptor"() {
    when:
    ProtoMapper.descriptorOf(com.google.protobuf.DynamicMessage)

    then:
    thrown(IllegalArgumentException)
  }
}
