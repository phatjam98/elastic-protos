package com.phatjam98.elasticprotos.mapping

import com.google.protobuf.DescriptorProtos.DescriptorProto
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto
import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FileDescriptor
import com.phatjam98.elasticprotos.Golden
import com.phatjam98.elasticprotos.proto.DynamicMode
import com.phatjam98.elasticprotos.proto.EsType
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.proto.example.InboxItem
import com.phatjam98.elasticprotos.proto.example.InnerMaps
import com.phatjam98.elasticprotos.proto.example.LooseValues
import com.phatjam98.elasticprotos.proto.example.TypedMaps
import com.phatjam98.elasticprotos.proto.test.Annotated
import com.phatjam98.elasticprotos.proto.test.Child
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
    'maps_typed'   | TypedMaps
    'maps_inner'   | InnerMaps
    'maps_loose_values' | LooseValues
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
    props.value_value == [type: 'object', enabled: false]
    props.list_value == [type: 'object', enabled: false]
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
    props.string_map == [type: 'flattened']
    props.message_map == [type: 'flattened']
    props.int_map == [type: 'flattened']
    definition.fields()['string_map'].kind() == FieldKind.FLATTENED
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

  def "a map field opts in to a dynamic object or nested with an explicit type"() {
    given:
    def overrides = MappingOverrides.builder()
        .forField(Composite.descriptor, 'string_map') { it.type = EsType.OBJECT }
        .forField(Composite.descriptor, 'message_map') { it.type = EsType.NESTED }
        .build()

    when:
    def definition = ProtoMapper.builder().overrides(overrides).build().map(Composite)
    def props = definition.mappings().properties

    then:
    props.string_map == [type: 'object', dynamic: true]
    props.message_map == [type: 'nested', dynamic: true]
    props.int_map == [type: 'flattened']
    definition.contentHash() != mapper.map(Composite).contentHash()
  }

  def "typed map templates are ordered deeper first, then by name"() {
    when:
    def definition = mapper.map(TypedMaps)
    List<String> names = definition.mappings().dynamic_templates.collect { (it as Map).keySet().first() }

    then:
    names == ['ep:children.*.items.quantity', 'ep:children.*.items.sku', 'ep:bags.*.tags', 'ep:children.*.count',
              'ep:children.*.items', 'ep:children.*.label', 'ep:children.*.name', 'ep:attributes.*', 'ep:bags.*',
              'ep:children.*', 'ep:counters.*', 'ep:due.*', 'ep:levels.*', 'ep:notes.*', 'ep:places.*']
    definition.fields().keySet().every { !it.contains('*') }
    definition.dynamicFields().keySet() == ['attributes.*', 'bags.*', 'bags.*.tags', 'children.*', 'children.*.count',
        'children.*.items', 'children.*.items.quantity', 'children.*.items.sku', 'children.*.label', 'children.*.name',
        'counters.*', 'due.*', 'levels.*', 'notes.*', 'places.*'] as Set
    definition.dynamicFields()['bags.*.tags'].kind() == FieldKind.FLATTENED
    definition.dynamicFields()['attributes.*'].kind() == FieldKind.FLATTENED
    definition.dynamicFields().values().every { it.isDynamic() && it.pattern() == it.path() }
    definition.dynamicFields()['children.*.label'].protoPath() == 'children.*.display_name'
    definition.dynamicFields()['notes.*'].kind() == FieldKind.TEXT
    definition.dynamicFields()['notes.*'].keywordSubfield()
    definition.nestedPaths().isEmpty()
  }

  def "typed maps resolve keys and instantiate nested paths"() {
    given:
    def definition = mapper.map(TypedMaps)

    expect:
    definition.resolve('counters.views').get() == new FieldInfo('counters.views', 'counters.views',
        FieldKind.LONG, false, false, 1f, TypedMaps.descriptor.findFieldByName('counters').messageType
        .findFieldByName('value'), 'counters.*')
    definition.resolve('children.k.label').get().protoPath() == 'children.k.display_name'
    definition.resolve('children.k').get().kind() == FieldKind.OBJECT
    definition.resolve('id').get().isDynamic() == false
    ['counters', 'labels'].every { !definition.resolve(it).get().isDynamic() }
    ['counters.', 'counters..x', 'counters.a.', 'counters.a.b', 'children.k.nope', 'nope.x', 'labels.env',
     'counters.*', 'counters.a*', 'children.*.count']
        .every { definition.resolve(it).isEmpty() }
    definition.typedMapValues('children.k.nope').get().path() == 'children.*'
    definition.typedMapValues('labels.env').isEmpty()
    definition.nestedAncestors('children.k.items.sku') == ['children.k.items']
    definition.nestedAncestors('children.k.count') == []
    definition.nestedAncestors('counters.views') == []
    mapper.map(Firm).typedMapValues('labels.env').isEmpty()
  }

  def "typed maps warn about the fields every key adds"() {
    expect:
    mapper.map(TypedMaps).warnings() == [
        'each distinct key of counters adds 1 field(s) at index time (index.mapping.total_fields.limit)',
        'each distinct key of notes adds 2 field(s) at index time (index.mapping.total_fields.limit)',
        'each distinct key of due adds 1 field(s) at index time (index.mapping.total_fields.limit)',
        'each distinct key of levels adds 1 field(s) at index time (index.mapping.total_fields.limit)',
        'each distinct key of places adds 1 field(s) at index time (index.mapping.total_fields.limit)',
        'each distinct key of children adds 7 field(s) at index time (index.mapping.total_fields.limit), 1 of them nested (index.mapping.nested_fields.limit, default 50)',
        'each distinct key of bags adds 2 field(s) at index time (index.mapping.total_fields.limit)',
        'each distinct key of attributes adds 1 field(s) at index time (index.mapping.total_fields.limit)',
    ]
  }

  def "value_type and the value options shape the template"() {
    given:
    def overrides = MappingOverrides.builder()
        .forField(Composite.descriptor, 'string_map') {
          it.type = EsType.OBJECT
          it.valueType = EsType.KEYWORD
          it.normalizer = 'lowercase'
          it.ignoreAbove = 32
          it.index = com.phatjam98.elasticprotos.proto.Bool3.FALSE
          it.addCopyTo('all')
          it.fullText = com.phatjam98.elasticprotos.proto.Bool3.TRUE
          it.boost = 3f
        }
        .forField(Composite.descriptor, 'int_map') {
          it.type = EsType.OBJECT
          it.valueType = EsType.LONG
          it.coerce = com.phatjam98.elasticprotos.proto.Bool3.FALSE
        }
        .forField(Composite.descriptor, 'message_map') {
          it.type = EsType.OBJECT
          it.valueType = EsType.NESTED
        }
        .build()

    when:
    def definition = ProtoMapper.builder().overrides(overrides).build().map(Composite)
    Map templates = definition.mappings().dynamic_templates.collectEntries { it }

    then:
    definition.mappings().properties.string_map == [type: 'object', dynamic: true]
    templates['ep:string_map.*'] == [path_match: 'string_map.*', mapping: [type: 'keyword', ignore_above: 32,
                                                                            normalizer: 'lowercase', index: false]]
    templates['ep:int_map.*'].mapping == [type: 'long', coerce: false]
    templates['ep:message_map.*'] == [path_match: 'message_map.*', match_mapping_type: 'object',
                                      mapping: [type: 'nested', dynamic: true]]
    templates['ep:message_map.*.grandchild'].mapping == [type: 'object', dynamic: true]
    templates['ep:message_map.*.grandchild.note'].mapping == [type: 'keyword', ignore_above: 256]
    definition.fullTextFields() == ['string_map.*': 3f]
    definition.nestedAncestors('message_map.k.rank') == ['message_map.k']
    definition.nestedAncestors('message_map.k.grandchild.note') == ['message_map.k']
    definition.resolve('message_map.k').get().kind() == FieldKind.NESTED
    definition.resolve('message_map.k').get().isDynamic()
    definition.warnings().contains('each distinct key of message_map adds 5 field(s) at index time'
        + ' (index.mapping.total_fields.limit), 1 of them nested (index.mapping.nested_fields.limit, default 50)')
  }

  def "full_text on a message-valued map adds its text leaves outside nested values"() {
    given:
    def textChild = { b -> b.type = EsType.OBJECT; b.fullText = com.phatjam98.elasticprotos.proto.Bool3.TRUE }
    def overrides = MappingOverrides.builder()
        .forField(Composite.descriptor, 'message_map', textChild)
        .forField(Child.descriptor, 'name') { it.type = EsType.TEXT }
        .build()
    def nestedValues = MappingOverrides.builder()
        .forField(Composite.descriptor, 'message_map') {
          it.type = EsType.OBJECT
          it.valueType = EsType.NESTED
          it.fullText = com.phatjam98.elasticprotos.proto.Bool3.TRUE
        }
        .forField(Child.descriptor, 'name') { it.type = EsType.TEXT }
        .build()

    expect:
    ProtoMapper.builder().overrides(overrides).build().map(Composite).fullTextFields() ==
        ['message_map.*.name': 1f, 'single_child.name': 1f, 'children.name': 1f, 'choice_child.name': 1f]
    ProtoMapper.builder().overrides(nestedValues).build().map(Composite).fullTextFields() ==
        ['single_child.name': 1f, 'children.name': 1f, 'choice_child.name': 1f]
  }

  def "a value cut off by maxDepth is a disabled object"() {
    given:
    def overrides = MappingOverrides.builder()
        .forField(Composite.descriptor, 'message_map') { it.type = EsType.OBJECT }.build()
    def shallow = ProtoMapper.builder().overrides(overrides)
        .defaults(MappingDefaults.standard().withMaxDepth(0)).build()

    when:
    def definition = shallow.map(Composite)

    then:
    definition.mappings().dynamic_templates.collectEntries { it }['ep:message_map.*'] ==
        [path_match: 'message_map.*', mapping: DISABLED]
    definition.warnings().contains(
        'each distinct key of message_map adds 1 field(s) at index time (index.mapping.total_fields.limit)')
    definition.dynamicFields().keySet() == ['message_map.*'] as Set
  }

  def "wrapper values are single values; Struct values are flattened and Any values disabled objects"() {
    given:
    def file = FileDescriptorProto.newBuilder().setName('values.proto').setPackage('values')
        .setSyntax('proto3')
        .addDependency('google/protobuf/struct.proto')
        .addDependency('google/protobuf/any.proto')
        .addDependency('google/protobuf/wrappers.proto')
        .addMessageType(DescriptorProto.newBuilder().setName('Values')
            .addField(mapField('structs', 1, '.values.Values.StructsEntry'))
            .addField(mapField('anys', 2, '.values.Values.AnysEntry'))
            .addField(mapField('wrapped', 3, '.values.Values.WrappedEntry'))
            .addNestedType(entry('StructsEntry', '.google.protobuf.Struct'))
            .addNestedType(entry('AnysEntry', '.google.protobuf.Any'))
            .addNestedType(entry('WrappedEntry', '.google.protobuf.Int32Value')))
        .build()
    Descriptor values = FileDescriptor.buildFrom(file, [com.google.protobuf.Struct.descriptor.file,
        com.google.protobuf.Any.descriptor.file, com.google.protobuf.Int32Value.descriptor.file] as FileDescriptor[])
        .findMessageTypeByName('Values')
    def overrides = MappingOverrides.builder()
    ['structs', 'anys', 'wrapped'].each { name ->
      overrides.forField(values, name) { it.type = EsType.OBJECT }
    }

    when:
    def definition = ProtoMapper.builder().overrides(overrides.build()).build().map(values)
    Map templates = definition.mappings().dynamic_templates.collectEntries { it }

    then:
    templates['ep:structs.*'] == [path_match: 'structs.*', mapping: [type: 'flattened']]
    templates['ep:wrapped.*'].mapping == [type: 'integer']
    templates['ep:anys.*'] == [path_match: 'anys.*', mapping: DISABLED]
    definition.warnings().contains('anys.*: google.protobuf.Any inside the values of a typed map is mapped as a'
        + ' disabled object (kept in _source, not indexed); the codec writes it only with CodecConfig.anyTypes'
        + ' and leaves it out otherwise')
    !definition.dynamicFields()['anys.*'].enabled()
    definition.dynamicFields()['structs.*'].enabled()
    definition.warnings().every { !it.contains('untyped') }
    definition.dynamicFields().keySet() == ['anys.*', 'structs.*', 'wrapped.*'] as Set
    definition.dynamicFields()['structs.*'].kind() == FieldKind.FLATTENED
    definition.dynamicFields()['anys.*'].kind() == FieldKind.OBJECT
    definition.ignoredPaths().isEmpty()
  }

  static FieldDescriptorProto mapField(String name, int number, String entryType) {
    FieldDescriptorProto.newBuilder().setName(name).setNumber(number)
        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(entryType)
        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED).build()
  }

  static DescriptorProto entry(String name, String valueType) {
    DescriptorProto.newBuilder().setName(name)
        .setOptions(com.google.protobuf.DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true))
        .addField(field('key', 1, FieldDescriptorProto.Type.TYPE_STRING))
        .addField(field('value', 2, FieldDescriptorProto.Type.TYPE_MESSAGE).toBuilder().setTypeName(valueType))
        .build()
  }

  @Unroll
  def "value_type #valueType on #field is rejected: #message"() {
    given:
    def overrides = MappingOverrides.builder()
        .forField(Composite.descriptor, field) {
          it.type = EsType.OBJECT
          it.valueType = valueType
        }.build()

    when:
    ProtoMapper.builder().overrides(overrides).build().map(Composite)

    then:
    MappingException e = thrown()
    e.path == field
    e.message == "${field}: ${message}".toString()

    where:
    field         | valueType        || message
    'string_map'  | EsType.OBJECT    || 'value_type OBJECT requires a message or map field'
    'int_map'     | EsType.GEO_SHAPE || 'value_type GEO_SHAPE requires a string field (WKT or GeoJSON) or phatjam98.elasticprotos.v1.GeoShape, not DOUBLE'
    'message_map' | EsType.LONG      || 'value_type LONG cannot be applied to message phatjam98.elasticprotos.test.Child'
    'message_map' | EsType.GEO_POINT || 'value_type GEO_POINT requires a string field (WKT or "lat,lon") or a message with lat/lon fields, not phatjam98.elasticprotos.test.Child'
    'string_map'  | EsType.IGNORE    || 'value_type IGNORE is not supported; set type: IGNORE to leave the map out'
  }

  def "value_type is ignored with a warning where it does not apply"() {
    given:
    def overrides = MappingOverrides.builder()
        .forField(Composite.descriptor, 'string_map') { it.valueType = EsType.TEXT }
        .forField(Composite.descriptor, 'single_child') { it.valueType = EsType.TEXT }
        .build()

    when:
    def definition = ProtoMapper.builder().overrides(overrides).build().map(Composite)

    then:
    definition.mappings().properties.string_map == [type: 'flattened']
    !definition.mappings().containsKey('dynamic_templates')
    definition.warnings() == [
        'single_child: value_type applies only to map fields mapped as OBJECT or NESTED and is ignored',
        'string_map: value_type applies only to map fields mapped as OBJECT or NESTED and is ignored']
  }

  def "flattened maps keep their mapping and content hash"() {
    expect: 'the hashes recorded before typed maps existed'
    mapper.map(Firm).contentHash() == 'e891e500'
    mapper.map(Composite).contentHash() == '5e01d47d'
    !mapper.map(Firm).mappings().containsKey('dynamic_templates')
    mapper.map(Firm).dynamicFields().isEmpty()

    and: 'TypedMaps with every map flattened has no templates'
    def flattened = MappingOverrides.builder()
    TypedMaps.descriptor.fields.findAll { it.mapField }.each { f ->
      flattened.forField(TypedMaps.descriptor, f.name) { it.type = EsType.FLATTENED }
    }
    def definition = ProtoMapper.builder().overrides(flattened.build()).build().map(TypedMaps)
    !definition.mappings().containsKey('dynamic_templates')
    definition.dynamicFields().isEmpty()
    definition.warnings() ==
        ['notes: value_type applies only to map fields mapped as OBJECT or NESTED and is ignored']
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
    props.labels == [type: 'flattened']
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

  // ------------------------------------------------------------------ field limits

  /**
   * Builds, at runtime, a message {@code Wide} with {@code scalars} string fields and {@code nested}
   * repeated {@code Item} fields, each Item holding one string.
   */
  static Descriptor wide(int scalars, int nested) {
    def item = DescriptorProto.newBuilder().setName('Item')
        .addField(field('label', 1, FieldDescriptorProto.Type.TYPE_STRING))
    def wide = DescriptorProto.newBuilder().setName('Wide')
    int number = 1
    scalars.times { wide.addField(field("s${it}", number++, FieldDescriptorProto.Type.TYPE_STRING)) }
    nested.times {
      wide.addField(field("n${it}", number++, FieldDescriptorProto.Type.TYPE_MESSAGE).toBuilder()
          .setTypeName('.limits.Item').setLabel(FieldDescriptorProto.Label.LABEL_REPEATED).build())
    }
    def file = FileDescriptorProto.newBuilder().setName('limits.proto').setPackage('limits')
        .setSyntax('proto3').addMessageType(item).addMessageType(wide).build()
    FileDescriptor.buildFrom(file, [] as FileDescriptor[]).findMessageTypeByName('Wide')
  }

  static FieldDescriptorProto field(String name, int number, FieldDescriptorProto.Type type) {
    FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type)
        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL).build()
  }

  static ProtoMapper limited(int nested, int total) {
    ProtoMapper.builder().defaults(MappingDefaults.standard().withMaxNestedFields(nested).withMaxTotalFields(total)).build()
  }

  def "the standard limits are the engines' defaults"() {
    expect:
    MappingDefaults.standard().maxNestedFields() == 50
    MappingDefaults.standard().maxTotalFields() == 1000
  }

  def "fields are counted like the engine counts them: containers and multi-fields included"() {
    expect: 'Document: 12 top-level fields, title.keyword, and parties with name, name.keyword, role and email'
    limited(1, 17).map(Document).nestedPaths() == ['parties'] as Set
  }

  def "a mapping one field over the total limit is rejected"() {
    when:
    limited(1, 16).map(Document)

    then:
    def e = thrown(MappingException)
    e.path == 'phatjam98.elasticprotos.example.Document'
    e.message.contains('maps 17 fields, more than maxTotalFields 16')
  }

  def "a generated message exactly at the limits maps; one more of either is rejected: #label"() {
    when:
    def definition = limited(nestedLimit, totalLimit).map(wide(scalars, nested))

    then:
    definition.nestedPaths().size() == nested

    where: 'each nested Item counts twice: the container and its label'
    label         | scalars | nested | nestedLimit | totalLimit
    'at both'     | 10      | 3      | 3           | 16
    'disabled'    | 2000    | 60     | 0           | 0
    'defaults'    | 990     | 5      | 50          | 1000
  }

  def "a generated message over a limit is rejected: #label"() {
    when:
    limited(nestedLimit, totalLimit).map(wide(scalars, nested))

    then:
    def e = thrown(MappingException)
    e.path == 'limits.Wide'
    e.message.contains(message)

    where:
    label           | scalars | nested | nestedLimit | totalLimit | message
    'nested'        | 0       | 4      | 3           | 0          | 'maps 4 nested fields, more than maxNestedFields 3 (index.mapping.nested_fields.limit)'
    'total'         | 17      | 0      | 0           | 16         | 'maps 17 fields, more than maxTotalFields 16 (index.mapping.total_fields.limit)'
    'default total' | 1001    | 0      | 50          | 1000       | 'more than maxTotalFields 1000'
    'default nested'| 0       | 51     | 50          | 1000       | 'maps 51 nested fields, more than maxNestedFields 50'
  }

  // ------------------------------------------------------------------ name validation

  def "a mapped-name override is validated: #label"() {
    given:
    def overrides = MappingOverrides.builder().forField(Scalars.descriptor, 'string_value') { it.name = name }.build()

    when:
    mapper.map(Scalars.descriptor, overrides)

    then:
    def e = thrown(MappingException)
    e.path == 'string_value'
    e.message.contains("elastic_field.name '${name}' must be non-empty and contain no '.', '*' or whitespace")

    where:
    label        | name
    'a dot'      | 'a.b'
    'a space'    | 'a b'
    'blank'      | ' '
    'a tab'      | 'x\ty'
    'a wildcard' | 'c*'
  }

  def "a valid mapped-name override renames the field"() {
    given:
    def overrides = MappingOverrides.builder().forField(Scalars.descriptor, 'string_value') { it.name = 'str-value_2' }.build()

    expect:
    mapper.map(Scalars.descriptor, overrides).mappings().properties['str-value_2'].type == 'keyword'
  }

  def "a naming prefix is lower-cased"() {
    expect:
    IndexNamingStrategy.prefixed('Dev').alias('documents') == 'dev-documents'
    IndexNamingStrategy.prefixed('QA1').indexName('documents', 'abc') == 'qa1-documents-vabc'
  }

  def "a naming prefix that cannot start an index name is rejected: #label"() {
    when:
    IndexNamingStrategy.prefixed(prefix)

    then:
    thrown(exception)

    where:
    label            | prefix   | exception
    'empty'          | ''       | IllegalArgumentException
    'null'           | null     | NullPointerException
    'leading -'      | '-dev'   | IllegalArgumentException
    'leading _'      | '_dev'   | IllegalArgumentException
    'leading +'      | '+dev'   | IllegalArgumentException
    'space'          | 'my env' | IllegalArgumentException
    'backslash'      | 'a\\b'   | IllegalArgumentException
    'slash'          | 'a/b'    | IllegalArgumentException
    'star'           | 'a*'     | IllegalArgumentException
    'question mark'  | 'a?'     | IllegalArgumentException
    'quote'          | 'a"'     | IllegalArgumentException
    'less than'      | 'a<'     | IllegalArgumentException
    'greater than'   | 'a>'     | IllegalArgumentException
    'pipe'           | 'a|b'    | IllegalArgumentException
    'comma'          | 'a,b'    | IllegalArgumentException
    'hash'           | 'a#b'    | IllegalArgumentException
    'colon'          | 'a:b'    | IllegalArgumentException
  }
}
