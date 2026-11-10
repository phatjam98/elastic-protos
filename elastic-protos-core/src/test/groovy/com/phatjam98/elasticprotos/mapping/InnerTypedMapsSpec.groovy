package com.phatjam98.elasticprotos.mapping

import com.google.protobuf.DescriptorProtos.DescriptorProto
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto
import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FileDescriptor
import com.phatjam98.elasticprotos.proto.EsType
import com.phatjam98.elasticprotos.proto.example.InnerMaps
import com.phatjam98.elasticprotos.proto.example.InnerPart
import com.phatjam98.elasticprotos.proto.example.InnerValue
import com.phatjam98.elasticprotos.proto.example.LooseItem
import com.phatjam98.elasticprotos.proto.example.LooseValues
import com.phatjam98.elasticprotos.proto.example.TypedMaps
import java.util.regex.Pattern
import spock.lang.Shared
import spock.lang.Specification

/** Maps inside the values of typed maps (ADR 0011). */
class InnerTypedMapsSpec extends Specification {

  static final Map DISABLED = [type: 'object', enabled: false]

  @Shared
  ProtoMapper mapper = ProtoMapper.withDefaults()

  @Shared
  IndexDefinition inner = mapper.map(InnerMaps)

  def "typed maps without inner maps, Struct or Any keep the 1.0.0-rc.3 hash"() {
    given: 'TypedMaps without bags (a map in the value) and attributes (Struct values)'
    def overrides = MappingOverrides.builder()
        .forField(TypedMaps.descriptor, 'bags') { it.type = EsType.IGNORE }
        .forField(TypedMaps.descriptor, 'attributes') { it.type = EsType.IGNORE }
        .build()

    expect: 'the hash computed by the 1.0.0-rc.3 mapper'
    ProtoMapper.builder().overrides(overrides).build().map(TypedMaps).contentHash() == '25d5676a'
  }

  def "inner fields follow the rule they follow anywhere else"() {
    when:
    Map templates = inner.mappings().dynamic_templates.collectEntries { it }

    then: 'a map is flattened, a typed map gets a container and key templates, Struct is flattened, Any disabled'
    templates['ep:outer.*.labels'] == [path_match: 'outer.*.labels', mapping: [type: 'flattened']]
    templates['ep:outer.*.metrics'] == [path_match: 'outer.*.metrics', match_mapping_type: 'object',
                                        mapping: [type: 'object', dynamic: true]]
    templates['ep:outer.*.metrics.*'] == [path_match: 'outer.*.metrics.*', mapping: [type: 'double']]
    templates['ep:outer.*.parts.*.counts.*'].mapping == [type: 'long']
    templates['ep:outer.*.parts.*.at'].mapping == [type: 'date', format: 'strict_date_optional_time']
    templates['ep:outer.*.parts.*.where'].mapping == [type: 'geo_point']
    templates['ep:outer.*.extra'] == [path_match: 'outer.*.extra', mapping: [type: 'flattened']]
    templates['ep:outer.*.payload'] == [path_match: 'outer.*.payload', mapping: DISABLED]
    templates['ep:outer.*.mask'] == [path_match: 'outer.*.mask', mapping: DISABLED]
    templates['ep:nested_outer.*'].mapping == [type: 'nested', dynamic: true]

    and: 'nothing is untyped and nothing is ignored'
    inner.warnings().every { !it.contains('untyped') }
    inner.ignoredPaths().isEmpty()
    inner.dynamicFields()['outer.*.labels'].kind() == FieldKind.FLATTENED
    inner.dynamicFields()['outer.*.metrics'].kind() == FieldKind.OBJECT
    inner.dynamicFields()['outer.*.parts.*.counts.*'].protoPath() == 'outer.*.parts.*.counts.*'
  }

  def "templates are ordered by segment count, deepest first, then by name"() {
    when:
    List<String> names = inner.mappings().dynamic_templates.collect { (it as Map).keySet().first() }
    List<Integer> segments = names.collect { it.split('\\.', -1).length }

    then:
    segments == segments.sort(false).reverse()
    names.first() == 'ep:outer.*.parts.*.counts.*'
    names.last() == 'ep:outer.*'
  }

  def "every concrete path is mapped by its own template, even with keys equal to field names"() {
    given: 'keys taken from every field name of the mapping, plus a plain one'
    List<Map> templates = inner.mappings().dynamic_templates.collect { (it as Map).entrySet().first() }
        .collect { [name: it.key, regex: glob(it.value.path_match), objects: it.value.match_mapping_type == 'object'] }
    Set<String> keys = (inner.dynamicFields().keySet()*.split('\\.').flatten() as Set) - '*' + 'k'

    when: 'each pattern is instantiated with every key combination and run through first-match-wins'
    List<String> misses = []
    int checked = 0
    inner.dynamicFields().each { pattern, info ->
      boolean object = info.kind().isContainer() || info.kind() in [FieldKind.FLATTENED, FieldKind.GEO_POINT]
      instances(pattern, keys as List).each { path ->
        checked++
        String winner = templates.find { (!it.objects || object) && it.regex.matcher(path).matches() }?.name
        if (winner != 'ep:' + pattern) {
          misses << "${path} -> ${winner}, expected ep:${pattern}".toString()
        }
        if (inner.resolve(path).get().pattern() != pattern) {
          misses << "resolve ${path} -> ${inner.resolve(path).get().pattern()}".toString()
        }
      }
    }

    then:
    checked > 1000
    misses.isEmpty()
  }

  def "a dynamic object inside the values would be captured by the value templates, so Struct stays flattened"() {
    given:
    List<Map> templates = inner.mappings().dynamic_templates.collect { (it as Map).entrySet().first() }
        .collect { [name: it.key, regex: glob(it.value.path_match)] }

    expect: 'a key "score" below outer.k.extra would match ep:outer.*.score if extra were a dynamic object'
    templates.find { it.regex.matcher('outer.k.extra.score').matches() }.name == 'ep:outer.*.score'
  }

  def "an explicit OBJECT or NESTED Struct inside the values is refused"() {
    given:
    def overrides = MappingOverrides.builder()
        .forField(InnerValue.descriptor, 'extra') { it.type = type }.build()

    when:
    ProtoMapper.builder().overrides(overrides).build().map(InnerMaps)

    then:
    MappingException e = thrown()
    e.message == "outer.*.extra: ${type} cannot be applied to google.protobuf.Struct inside the values of a" +
        " typed map: the map's dynamic templates would also match its keys; use FLATTENED"

    where:
    type << [EsType.OBJECT, EsType.NESTED]
  }

  def "structKind OBJECT falls back to flattened inside the values, with a warning"() {
    given:
    def objectStructs = ProtoMapper.builder()
        .defaults(MappingDefaults.standard().withStructKind(FieldKind.OBJECT)).build()

    when:
    def definition = objectStructs.map(InnerMaps)

    then:
    definition.mappings().dynamic_templates.collectEntries { it }['ep:outer.*.extra'].mapping == [type: 'flattened']
    definition.warnings().contains('outer.*.extra: google.protobuf.Struct inside the values of a typed map is' +
        ' mapped as flattened, not OBJECT (MappingDefaults.structKind), since the map\'s dynamic templates' +
        ' would also match its keys')
  }

  def "an explicit type on Any inside the values is refused; IGNORE leaves it out"() {
    when:
    ProtoMapper.builder().overrides(MappingOverrides.builder()
        .forField(InnerValue.descriptor, 'payload') { it.type = EsType.OBJECT }.build()).build().map(InnerMaps)

    then:
    MappingException e = thrown()
    e.message == 'outer.*.payload: OBJECT cannot be applied to google.protobuf.Any inside the values of a' +
        ' typed map, which is mapped as a disabled object; set type: IGNORE to leave it out'

    when:
    def ignored = ProtoMapper.builder().overrides(MappingOverrides.builder()
        .forField(InnerValue.descriptor, 'payload') { it.type = EsType.IGNORE }.build()).build().map(InnerMaps)

    then:
    ignored.ignoredPaths() == ['outer.*.payload'] as Set
    !ignored.dynamicFields().containsKey('outer.*.payload')
  }

  def "a field name holding * is refused everywhere, since it would act as a template wildcard"() {
    when: 'inside the values'
    ProtoMapper.builder().overrides(MappingOverrides.builder()
        .forField(InnerValue.descriptor, 'score') { it.name = 's*' }.build()).build().map(InnerMaps)

    then:
    MappingException e = thrown()
    e.message == "outer.*.score: elastic_field.name 's*' must be non-empty and contain no '.', '*' or whitespace"

    when: 'a typed map renamed to c* would emit ep:c*.*, which matches children.k before its own template'
    ProtoMapper.builder().overrides(MappingOverrides.builder()
        .forField(TypedMaps.descriptor, 'counters') { it.name = 'c*' }.build()).build().map(TypedMaps)

    then:
    e = thrown()
    e.message == "counters: elastic_field.name 'c*' must be non-empty and contain no '.', '*' or whitespace"

    when: 'by path override too'
    ProtoMapper.builder().overrides(MappingOverrides.builder()
        .forPath('counters') { it.name = '*' }.build()).build().map(TypedMaps)

    then:
    thrown(MappingException)
  }

  def "Value and ListValue are disabled objects at the top level and inside typed map values; Struct is flattened"() {
    when:
    def definition = mapper.map(LooseValues)
    Map props = definition.mappings().properties
    Map templates = definition.mappings().dynamic_templates.collectEntries { it }

    then:
    props.value == DISABLED
    props.list == DISABLED
    props.struct == [type: 'flattened']
    templates['ep:values.*'] == [path_match: 'values.*', mapping: DISABLED]
    templates['ep:items.*.value'] == [path_match: 'items.*.value', mapping: DISABLED]
    templates['ep:items.*.list'] == [path_match: 'items.*.list', mapping: DISABLED]
    !definition.field('value').get().enabled()
    !definition.resolve('items.k.list').get().enabled()
    definition.resolve('items.k.n').get().enabled()
    definition.warnings().every { !it.contains('Value') }

    and: 'structKind governs Struct only'
    ProtoMapper.builder().defaults(MappingDefaults.standard().withStructKind(FieldKind.OBJECT)).build()
        .map(LooseValues).mappings().properties.with { it.value == DISABLED && it.struct == [type: 'object', dynamic: true] }
  }

  def "an explicit type on a top-level Value is kept, with a warning; inside typed map values it is refused"() {
    when:
    def definition = ProtoMapper.builder().overrides(MappingOverrides.builder()
        .forField(LooseValues.descriptor, 'value') { it.type = EsType.FLATTENED }
        .forField(LooseValues.descriptor, 'list') { it.type = EsType.OBJECT }.build()).build().map(LooseValues)

    then:
    definition.mappings().properties.value == [type: 'flattened']
    definition.mappings().properties.list == [type: 'object', dynamic: true]
    definition.warnings().contains('value: google.protobuf.Value mapped as FLATTENED rejects documents whose value'
        + ' is not an object (a scalar, an array of scalars or null)')
    definition.warnings().contains('list: google.protobuf.ListValue mapped as OBJECT rejects documents whose value'
        + ' is not an object (a scalar, an array of scalars or null)')

    when:
    ProtoMapper.builder().overrides(MappingOverrides.builder()
        .forField(LooseItem.descriptor, 'value') { it.type = EsType.FLATTENED }.build()).build().map(LooseValues)

    then:
    MappingException e = thrown()
    e.message == 'items.*.value: FLATTENED cannot be applied to google.protobuf.Value inside the values of a typed' +
        ' map, which is mapped as a disabled object; set type: IGNORE to leave it out'

    when: 'value_type on a map of Value'
    ProtoMapper.builder().overrides(MappingOverrides.builder()
        .forField(LooseValues.descriptor, 'values') { it.type = EsType.OBJECT; it.valueType = EsType.FLATTENED }.build())
        .build().map(LooseValues)

    then:
    thrown(MappingException)
  }

  def "resolve instantiates every key of a doubly keyed path"() {
    expect:
    inner.resolve('outer.a.metrics.m').get().with {
      it.path() == 'outer.a.metrics.m' && it.protoPath() == 'outer.a.metrics.m' && it.kind() == FieldKind.DOUBLE &&
          it.pattern() == 'outer.*.metrics.*' && it.isDynamic()
    }
    inner.resolve('outer.metrics.metrics.metrics').get().pattern() == 'outer.*.metrics.*'
    inner.resolve('outer.a.parts.b.counts.c').get().kind() == FieldKind.LONG
    inner.resolve('outer.a.parts.b').get().kind() == FieldKind.OBJECT
    inner.resolve('outer.a.parts').get().kind() == FieldKind.OBJECT
    inner.resolve('outer.a.labels').get().kind() == FieldKind.FLATTENED
    inner.resolve('outer.a.payload').get().kind() == FieldKind.OBJECT
    inner.resolve('nested_outer.k.counts.c').get().kind() == FieldKind.LONG

    and: 'empty, starred or extra segments do not resolve'
    ['outer.a.metrics', 'outer.a.metrics.', 'outer..metrics.m', 'outer.a.metrics.*', 'outer.*.metrics.m',
     'outer.a.metrics.m.x', 'outer.a.nope.m', 'outer.a.labels.env'].each {
      assert it == 'outer.a.metrics' || inner.resolve(it).isEmpty()
    }

    and: 'the values of the innermost map a path runs through'
    inner.typedMapValues('outer.a.parts.b.nope').get().path() == 'outer.*.parts.*'
    inner.typedMapValues('outer.a.metrics.m.x').get().path() == 'outer.*.metrics.*'
    inner.typedMapValues('outer.a.nope').get().path() == 'outer.*'
    inner.typedMapValues('id').isEmpty()
  }

  def "nested ancestors are instantiated at every level, NESTED inside and outside"() {
    given: 'outer values nested, parts a nested container with nested values'
    def overrides = MappingOverrides.builder()
        .forField(InnerMaps.descriptor, 'outer') { it.type = EsType.OBJECT; it.valueType = EsType.NESTED }
        .forField(InnerValue.descriptor, 'parts') { it.type = EsType.NESTED; it.valueType = EsType.NESTED }
        .build()
    def definition = ProtoMapper.builder().overrides(overrides).build().map(InnerMaps)
    Map templates = definition.mappings().dynamic_templates.collectEntries { it }

    expect:
    templates['ep:outer.*.parts'].mapping == [type: 'nested', dynamic: true]
    templates['ep:outer.*.parts.*'].mapping == [type: 'nested', dynamic: true]
    definition.nestedAncestors('outer.a.parts.b.qty') == ['outer.a', 'outer.a.parts', 'outer.a.parts.b']
    definition.nestedAncestors('outer.a.parts.b.counts.c') == ['outer.a', 'outer.a.parts', 'outer.a.parts.b']
    definition.nestedAncestors('outer.a.score') == ['outer.a']
    definition.nestedAncestors('outer.a') == []
    definition.nestedAncestors('outer.*.parts.*.qty') == []
    inner.nestedAncestors('outer.a.parts.b.qty') == []
    inner.nestedAncestors('nested_outer.k.counts.c') == ['nested_outer.k']
    definition.warnings().contains('each distinct key of outer.*.parts adds 5 field(s) at index time' +
        ' (index.mapping.total_fields.limit), 1 of them nested (index.mapping.nested_fields.limit, default 50)')
    definition.warnings().contains('each distinct key of outer adds 8 field(s) at index time' +
        ' (index.mapping.total_fields.limit), 2 of them nested (index.mapping.nested_fields.limit, default 50)')
  }

  def "a value message that recurses through an inner typed map ends in a disabled object"() {
    given: 'Node { map<string, Node> children [OBJECT]; int64 size } under Root { map<string, Node> nodes [OBJECT] }'
    def file = FileDescriptorProto.newBuilder().setName('rec.proto').setPackage('rec').setSyntax('proto3')
        .addMessageType(DescriptorProto.newBuilder().setName('Root')
            .addField(mapField('nodes', 1, '.rec.Root.NodesEntry'))
            .addNestedType(entry('NodesEntry', '.rec.Node')))
        .addMessageType(DescriptorProto.newBuilder().setName('Node')
            .addField(mapField('children', 1, '.rec.Node.ChildrenEntry'))
            .addField(FieldDescriptorProto.newBuilder().setName('size').setNumber(2)
                .setType(FieldDescriptorProto.Type.TYPE_INT64))
            .addNestedType(entry('ChildrenEntry', '.rec.Node')))
        .build()
    FileDescriptor fd = FileDescriptor.buildFrom(file, [] as FileDescriptor[])
    Descriptor root = fd.findMessageTypeByName('Root')
    Descriptor node = fd.findMessageTypeByName('Node')
    def overrides = MappingOverrides.builder()
        .forField(root, 'nodes') { it.type = EsType.OBJECT }
        .forField(node, 'children') { it.type = EsType.OBJECT }
        .build()

    when:
    def definition = ProtoMapper.builder().overrides(overrides).build().map(root)
    Map templates = definition.mappings().dynamic_templates.collectEntries { it }

    then:
    templates['ep:nodes.*.size'].mapping == [type: 'long']
    templates['ep:nodes.*.children'].mapping == [type: 'object', dynamic: true]
    templates['ep:nodes.*.children.*'].mapping == DISABLED
    definition.warnings().any { it.startsWith('nodes.*.children.*: recursive message rec.Node') }
  }

  def "maxDepth cuts inner typed map values off as disabled objects"() {
    when:
    def definition = ProtoMapper.builder().defaults(MappingDefaults.standard().withMaxDepth(1)).build()
        .map(InnerMaps)
    Map templates = definition.mappings().dynamic_templates.collectEntries { it }

    then:
    templates['ep:outer.*.parts.*'].mapping == DISABLED
    !templates.containsKey('ep:outer.*.parts.*.qty')
    templates['ep:outer.*.metrics.*'].mapping == [type: 'double']
  }

  def "the inner part message is the same descriptor in both maps"() {
    expect:
    inner.dynamicFields()['outer.*.parts.*'].field().messageType == InnerPart.descriptor
    inner.dynamicFields()['nested_outer.*'].field().messageType == InnerPart.descriptor
  }

  /** Turns a path_match glob into a regex the way the engines match it: {@code *} spans dots. */
  static Pattern glob(String pathMatch) {
    Pattern.compile(pathMatch.split('\\*', -1).collect { Pattern.quote(it) }.join('.*'))
  }

  /** Every instantiation of a pattern with the given keys. */
  static List<String> instances(String pattern, List<String> keys) {
    List<String> paths = ['']
    pattern.split('\\.').each { segment ->
      List<String> options = segment == '*' ? keys : [segment]
      paths = paths.collectMany { prefix -> options.collect { prefix.isEmpty() ? it : prefix + '.' + it } }
    }
    paths
  }

  static FieldDescriptorProto mapField(String name, int number, String entryType) {
    FieldDescriptorProto.newBuilder().setName(name).setNumber(number)
        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(entryType)
        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED).build()
  }

  static DescriptorProto entry(String name, String valueType) {
    DescriptorProto.newBuilder().setName(name)
        .setOptions(com.google.protobuf.DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true))
        .addField(FieldDescriptorProto.newBuilder().setName('key').setNumber(1)
            .setType(FieldDescriptorProto.Type.TYPE_STRING))
        .addField(FieldDescriptorProto.newBuilder().setName('value').setNumber(2)
            .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(valueType))
        .build()
  }
}
