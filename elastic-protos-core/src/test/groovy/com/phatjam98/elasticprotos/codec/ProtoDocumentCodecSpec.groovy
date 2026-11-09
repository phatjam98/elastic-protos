package com.phatjam98.elasticprotos.codec

import com.google.protobuf.ByteString
import com.google.protobuf.Duration
import com.google.protobuf.Int64Value
import com.google.protobuf.ListValue
import com.google.protobuf.StringValue
import com.google.protobuf.Struct
import com.google.protobuf.Timestamp
import com.google.protobuf.Value
import com.google.protobuf.util.Timestamps
import com.phatjam98.elasticprotos.engine.Hit
import com.phatjam98.elasticprotos.mapping.MappingOverrides
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.EsType
import com.phatjam98.elasticprotos.proto.GeoShape
import com.phatjam98.elasticprotos.proto.LatLon
import com.phatjam98.elasticprotos.proto.example.Address
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.DocumentType
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.proto.example.FirmStatus
import com.phatjam98.elasticprotos.proto.example.InboxItem
import com.phatjam98.elasticprotos.proto.example.InboxStatus
import com.phatjam98.elasticprotos.proto.example.Office
import com.phatjam98.elasticprotos.proto.example.Party
import com.phatjam98.elasticprotos.proto.example.PartyRole
import com.phatjam98.elasticprotos.proto.example.TypedBag
import com.phatjam98.elasticprotos.proto.example.TypedChild
import com.phatjam98.elasticprotos.proto.example.TypedItem
import com.phatjam98.elasticprotos.proto.example.TypedMapLevel
import com.phatjam98.elasticprotos.proto.example.TypedMaps
import com.phatjam98.elasticprotos.proto.test.Annotated
import com.phatjam98.elasticprotos.proto.test.Child
import com.phatjam98.elasticprotos.proto.test.Color
import com.phatjam98.elasticprotos.proto.test.Composite
import com.phatjam98.elasticprotos.proto.test.Coordinates
import com.phatjam98.elasticprotos.proto.test.Geo
import com.phatjam98.elasticprotos.proto.test.Grandchild
import com.phatjam98.elasticprotos.proto.test.IdByOption
import com.phatjam98.elasticprotos.proto.test.Scalars
import com.phatjam98.elasticprotos.proto.test.TreeNode
import com.phatjam98.elasticprotos.proto.test.WellKnown
import com.phatjam98.elasticprotos.util.Json
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class ProtoDocumentCodecSpec extends Specification {

  @Shared
  ProtoDocumentCodec codec = ProtoDocumentCodec.of(ProtoMapper.withDefaults())

  @Shared
  ProtoDocumentCodec sparse = ProtoDocumentCodec.of(ProtoMapper.withDefaults(), CodecConfig.standard().withIncludeDefaults(false))

  static final Timestamp FILED = Timestamps.parse('2024-03-04T05:06:07.123Z')

  static Document document() {
    Document.newBuilder()
        .setId('doc-1')
        .setFirmId('firm-1')
        .setTitle('Motion to dismiss')
        .setBody('The defendant moves to dismiss the complaint.')
        .setType(DocumentType.MOTION)
        .addTags('civil').addTags('urgent')
        .addParties(Party.newBuilder().setName('Acme Corp').setRole(PartyRole.DEFENDANT).setEmail('legal@acme.example'))
        .addParties(Party.newBuilder().setName('Jane Doe').setRole(PartyRole.PLAINTIFF))
        .setFiledAt(FILED)
        .setSizeBytes(9007199254740993L)
        .setChecksum(ByteString.copyFromUtf8('abc'))
        .setMetadata(Struct.newBuilder().putFields('pages', Value.newBuilder().setNumberValue(12).build()))
        .setInternalNotes('do not index me')
        .setRelevance(0.5d)
        .build()
  }

  static Firm firm() {
    Firm.newBuilder()
        .setId('firm-1')
        .setName('Dewey, Cheatem & Howe')
        .setDomain('dch.example')
        .setStatus(FirmStatus.ACTIVE)
        .setHeadquarters(Address.newBuilder().setCity('Las Vegas').setState('NV')
            .setLocation(LatLon.newBuilder().setLat(36.17).setLon(-115.14)))
        .addOffices(Office.newBuilder().setName('Reno').setHeadcount(12)
            .setAddress(Address.newBuilder().setCity('Reno').setLocation(LatLon.newBuilder().setLat(39.53).setLon(-119.81))))
        .putLabels('tier', 'gold')
        .setCreatedAt(FILED)
        .build()
  }

  def "a Document becomes a document shaped for the index"() {
    when:
    def doc = codec.toDocument(document())

    then:
    doc.keySet() == ['body', 'checksum', 'filed_at', 'firm_id', 'id', 'metadata', 'parties', 'relevance', 'size_bytes', 'tags', 'title', 'type'] as Set
    doc.id == 'doc-1'
    doc.title == 'Motion to dismiss'
    doc.type == 'MOTION'
    doc.tags == ['civil', 'urgent']
    doc.filed_at == '2024-03-04T05:06:07.123Z'
    doc.size_bytes == 9007199254740993L
    doc.size_bytes instanceof Long
    doc.checksum == 'YWJj'
    doc.metadata == [pages: 12.0d]
    doc.relevance == 0.5d
    doc.parties == [[email: 'legal@acme.example', name: 'Acme Corp', role: 'DEFENDANT'], [email: '', name: 'Jane Doe', role: 'PLAINTIFF']]
    !doc.containsKey('internal_notes')
    Json.canonical(doc) == codec.toJson(document())
  }

  def "a Firm's geo points and map take the engine's shape"() {
    when:
    def doc = codec.toDocument(firm())

    then:
    doc.headquarters == [city: 'Las Vegas', location: [lat: 36.17d, lon: -115.14d], postal_code: '', state: 'NV', street: '']
    doc.offices == [[address: [city: 'Reno', location: [lat: 39.53d, lon: -119.81d], postal_code: '', state: '', street: ''], headcount: 12, name: 'Reno']]
    doc.labels == [tier: 'gold']
    doc.status == 'ACTIVE'
    doc.created_at == '2024-03-04T05:06:07.123Z'
    !doc.containsKey('updated_at')

    and: 'without defaults only set values are written'
    sparse.toDocument(firm()).headquarters == [city: 'Las Vegas', location: [lat: 36.17d, lon: -115.14d], state: 'NV']
  }

  @Unroll
  def "#name round-trips through a document"() {
    when:
    def doc = codec.toDocument(message)
    def back = codec.fromDocument(doc, message.class)

    then:
    back == expected

    where:
    name       | message                                                                 | expected
    'Firm'     | firm()                                                                  | firm()
    'Document' | document()                                                              | document().toBuilder().clearInternalNotes().build()
    'Scalars'  | Scalars.newBuilder().setInt64Value(Long.MAX_VALUE).setUint64Value(-1L).setStringValue('s').setEnumValue(Color.RED).addRepeatedInts(1).addRepeatedInts(2).setBytesValue(ByteString.copyFrom(new byte[] {1, 2, 3})).setDoubleValue(1.5d).setBoolValue(true).build() | message
    'InboxItem' | InboxItem.newBuilder().setId('in-1').setSubject('hi').setStatus(InboxStatus.UNREAD).setDocument(document()).addRecipientEmails('a@b.c').build() | InboxItem.newBuilder().setId('in-1').setSubject('hi').setStatus(InboxStatus.UNREAD).setDocument(document().toBuilder().clearInternalNotes()).addRecipientEmails('a@b.c').build()
    'Composite' | Composite.newBuilder().setSingleChild(Child.newBuilder().setName('c').setRank(2)).addChildren(Child.newBuilder().setName('k')).putStringMap('a', 'b').putMessageMap('m', Child.newBuilder().setName('v').setGrandchild(Grandchild.newBuilder().setNote('n')).build()).putIntMap(1, 2.5d).setChoiceInt(7L).build() | message
    'defaults' | Scalars.getDefaultInstance()                                            | message
    'empty Firm' | Firm.getDefaultInstance()                                             | message
    'Composite with default oneof' | Composite.newBuilder().setChoiceChild(Child.getDefaultInstance()).build() | message
    'TreeNode' | TreeNode.newBuilder().setLabel('root').addChildren(TreeNode.newBuilder().setLabel('leaf')).build() | message
  }

  def "64-bit integers are numbers by default and strings when asked"() {
    given:
    def message = Scalars.newBuilder().setInt64Value(123L).setUint64Value(5L).build()
    def strings = ProtoDocumentCodec.of(ProtoMapper.withDefaults(), CodecConfig.standard().withInt64AsNumber(false))

    expect:
    sparse.toDocument(message) == [int64_value: 123L, uint64_value: '5']
    strings.toDocument(message).subMap(['int64_value', 'uint64_value', 'sint64_value']) == [int64_value: '123', uint64_value: '5', sint64_value: '0']
    codec.toDocument(message).sint64_value == 0L
    codec.fromDocument([int64_value: '123', uint64_value: '5'], Scalars) == message
    codec.fromDocument([int64_value: 123, uint64_value: 5], Scalars) == message
  }

  def "a 64-bit value that does not fit the mapped long is a CodecException naming the field"() {
    given: 'uint64 forced to LONG; 2^64 - 1 prints as an unsigned string'
    def forced = ProtoDocumentCodec.of(ProtoMapper.builder().overrides(MappingOverrides.builder()
        .forField(Scalars.descriptor, 'uint64_value') { it.type = EsType.LONG }.build()).build())
    def message = Scalars.newBuilder().setUint64Value(-1L).setFixed64Value(-1L).build()

    expect: 'fixed64 maps to keyword, so its full range is a string'
    sparse.toDocument(message) == [uint64_value: '18446744073709551615', fixed64_value: '18446744073709551615']
    codec.fromDocument(codec.toDocument(message), Scalars) == message

    when:
    forced.toDocument(message)

    then:
    def e = thrown(CodecException)
    e.message.contains('uint64_value')
    e.message.contains('18446744073709551615')
    e.message.contains('long')
    e.cause instanceof NumberFormatException
  }

  def "a lat/lon message always writes both coordinates, even when they are 0"() {
    expect:
    codec.toDocument(Geo.newBuilder().setPoint(LatLon.newBuilder().setLon(10)).build()).point == [lat: 0.0d, lon: 10.0d]
    codec.toDocument(Geo.newBuilder().setPoint(LatLon.newBuilder().setLat(-5)).build()).point == [lat: -5.0d, lon: 0.0d]
    codec.toDocument(Geo.newBuilder().setPoint(LatLon.getDefaultInstance()).build()).point == [lat: 0.0d, lon: 0.0d]
    codec.toDocument(Geo.newBuilder().setCoordinates(Coordinates.getDefaultInstance()).build()).coordinates == [lat: 0.0d, lon: 0.0d]
    !codec.toDocument(Geo.getDefaultInstance()).containsKey('point')

    and: 'it reads back to the same message'
    codec.fromDocument([point: [lat: 0.0d, lon: 10.0d]], Geo).point == LatLon.newBuilder().setLon(10).build()
  }

  def "proto3 default values are written by default"() {
    when:
    def doc = codec.toDocument(Scalars.getDefaultInstance())
    def json = codec.toJson(Scalars.getDefaultInstance())

    then: 'every scalar, enum and repeated field without presence is present with its default'
    codec.config().includeDefaults()
    CodecConfig.standard().includeDefaults()
    doc == [bool_value: false, double_value: 0.0d, enum_value: 'COLOR_UNSPECIFIED', fixed32_value: 0L,
            fixed64_value: '0', float_value: 0.0d, int32_value: 0, int64_value: 0L, repeated_ints: [],
            repeated_strings: [], sfixed32_value: 0, sfixed64_value: 0L, sint32_value: 0, sint64_value: 0L,
            string_value: '', uint32_value: 0L, uint64_value: '0']
    json.contains('"bool_value":false')
    json.contains('"int32_value":0')
    json.contains('"string_value":""')
    json.contains('"enum_value":"COLOR_UNSPECIFIED"')
    json.contains('"repeated_strings":[]')

    and: 'an empty bytes value carries nothing and is left out'
    !doc.containsKey('bytes_value')

    and: 'it reads back to the same message'
    codec.fromDocument(doc, Scalars) == Scalars.getDefaultInstance()
  }

  def "defaults are omitted when includeDefaults is false"() {
    expect:
    !sparse.config().includeDefaults()
    sparse.toDocument(Scalars.getDefaultInstance()) == [:]
    sparse.toDocument(Composite.getDefaultInstance()) == [:]
    sparse.toDocument(Scalars.newBuilder().setBoolValue(true).build()) == [bool_value: true]
  }

  def "unset fields with presence stay absent when defaults are written"() {
    expect: 'unset sub-messages, timestamps, wrappers, Struct and Any are not written, not even as {}'
    codec.toDocument(WellKnown.getDefaultInstance()) == [:]

    and: 'maps and repeated fields are written empty; an unset message and oneof are absent'
    codec.toDocument(Composite.getDefaultInstance()) == [children: [], int_map: [:], message_map: [:], string_map: [:]]

    and: 'only the set oneof member is written, even when it holds a default'
    codec.toDocument(Composite.newBuilder().setChoiceInt(0L).build()).subMap(['choice_int', 'choice_string', 'choice_child']) == [choice_int: 0L]
    codec.toDocument(Composite.newBuilder().setChoiceChild(Child.getDefaultInstance()).build()).choice_child == [name: '', rank: 0]

    and: 'a set sub-message is written with its own defaults, its unset children stay absent'
    codec.toDocument(Composite.newBuilder().setSingleChild(Child.getDefaultInstance()).build()).single_child == [name: '', rank: 0]

    and: 'proto3 optional, Struct, bytes and timestamps of an empty Document are absent; ignored fields too'
    codec.toDocument(Document.getDefaultInstance()) == [body: '', firm_id: '', id: '', parties: [], size_bytes: 0L, tags: [], title: '', type: 'DOCUMENT_TYPE_UNSPECIFIED']

    and: 'an empty Firm has no headquarters object and no epoch timestamps'
    codec.toDocument(Firm.getDefaultInstance()) == [domain: '', id: '', labels: [:], name: '', offices: [], status: 'FIRM_STATUS_UNSPECIFIED']
  }

  def "an empty string is left out where the engine would reject it"() {
    expect: 'string fields mapped as geo_point, geo_shape, date and ip'
    codec.toDocument(Geo.getDefaultInstance()) == [:]
    !codec.toDocument(Annotated.getDefaultInstance()).containsKey('birthday')
    !codec.toDocument(Annotated.getDefaultInstance()).containsKey('ip')

    and: 'a GeoShape message with neither wkt nor geojson is left out'
    !codec.toDocument(Geo.newBuilder().setShape(GeoShape.getDefaultInstance()).build()).containsKey('shape')

    and: 'keyword and text fields keep it'
    codec.toDocument(Annotated.getDefaultInstance()).subMap(['code', 'title']) == [code: '', title: '']
  }

  def "maps are plain JSON objects with string keys and read back"() {
    given:
    def message = Composite.newBuilder()
        .putStringMap('env', 'prod')
        .putMessageMap('k', Child.newBuilder().setName('c').build())
        .putIntMap(7, 1.5d)
        .build()

    when:
    def doc = codec.toDocument(message)

    then:
    doc.string_map == [env: 'prod']
    doc.message_map == [k: [name: 'c', rank: 0]]
    doc.int_map == ['7': 1.5d]
    codec.fromDocument(doc, Composite) == message
  }

  def "well-known types follow protobuf's JSON mapping and Any is dropped"() {
    given:
    def message = WellKnown.newBuilder()
        .setTimestampValue(FILED)
        .setDurationValue(Duration.newBuilder().setSeconds(3).setNanos(500000000))
        .setStringWrapper(StringValue.of('w'))
        .setInt64Wrapper(Int64Value.of(42L))
        .setStructValue(Struct.newBuilder().putFields('k', Value.newBuilder().setStringValue('v').build()))
        .setListValue(ListValue.newBuilder().addValues(Value.newBuilder().setBoolValue(true)))
        .setAnyValue(com.google.protobuf.Any.pack(StringValue.of('packed')))
        .build()

    when:
    def doc = codec.toDocument(message)

    then:
    doc.timestamp_value == '2024-03-04T05:06:07.123Z'
    doc.duration_value == '3.500s'
    doc.string_wrapper == 'w'
    doc.int64_wrapper == 42L
    doc.struct_value == [k: 'v']
    doc.list_value == [true]
    !doc.containsKey('any_value')
    codec.fromDocument(doc, WellKnown) == message.toBuilder().clearAnyValue().build()
  }

  def "epoch millis are accepted for timestamps when reading back"() {
    expect:
    codec.fromDocument([timestamp_value: FILED.seconds * 1000 + 123], WellKnown).timestampValue == FILED
  }

  def "geo fields are written in engine shape and read back from every accepted shape"() {
    given:
    def message = Geo.newBuilder()
        .setPoint(LatLon.newBuilder().setLat(1.5).setLon(2.5))
        .setShape(GeoShape.newBuilder().setWkt('POINT (2.5 1.5)'))
        .setWktPoint('POINT (2.5 1.5)')
        .setWktShape('{"coordinates":[2.5,1.5],"type":"Point"}')
        .setCoordinates(Coordinates.newBuilder().setLatitude(1.5).setLongitude(2.5))
        .build()

    when:
    def doc = codec.toDocument(message)

    then:
    doc.point == [lat: 1.5d, lon: 2.5d]
    doc.shape == 'POINT (2.5 1.5)'
    doc.wkt_point == 'POINT (2.5 1.5)'
    doc.wkt_shape == [type: 'Point', coordinates: [2.5d, 1.5d]]
    doc.coordinates == [lat: 1.5d, lon: 2.5d]

    and: 'a GeoShape with GeoJSON becomes an object and comes back as a GeoJSON string'
    def geojson = Geo.newBuilder().setShape(GeoShape.newBuilder().setGeojson('{"type":"Point","coordinates":[2.5,1.5]}')).build()
    codec.toDocument(geojson).shape == [type: 'Point', coordinates: [2.5d, 1.5d]]
    Json.parse(codec.fromDocument(codec.toDocument(geojson), Geo).shape.geojson) == [type: 'Point', coordinates: [2.5d, 1.5d]]

    and: 'reading back'
    codec.fromDocument(doc, Geo) == message
    codec.fromDocument([point: [2.5, 1.5]], Geo).point == message.point
    codec.fromDocument([point: '1.5,2.5'], Geo).point == message.point
    codec.fromDocument([point: 'POINT (2.5 1.5)'], Geo).point == message.point
    codec.fromDocument([coordinates: [lon: 2.5, lat: 1.5]], Geo).coordinates == message.coordinates
    codec.fromDocument([wkt_point: [lat: 1.5, lon: 2.5]], Geo).wktPoint == '1.5,2.5'
    codec.fromDocument([wkt_point: [2.5, 1.5]], Geo).wktPoint == '1.5,2.5'
    codec.fromDocument([wkt_shape: [type: 'Point', coordinates: [2.5, 1.5]]], Geo).wktShape == '{"coordinates":[2.5,1.5],"type":"Point"}'
    codec.fromDocument([shape: 'LINESTRING (0 0, 1 1)'], Geo).shape.wkt == 'LINESTRING (0 0, 1 1)'
  }

  def "a value that is not a geo point is rejected"() {
    when:
    codec.fromDocument([point: 42], Geo)

    then:
    thrown(CodecException)
  }

  def "renamed and ignored fields follow the index definition"() {
    given:
    def message = Annotated.newBuilder().setId('a').setRenamed('r').setSecret('s').setTitle('t').build()

    when:
    def doc = codec.toDocument(message)

    then:
    sparse.toDocument(message) == [id: 'a', renamed_field: 'r', title: 't']
    !doc.containsKey('secret')
    doc.renamed_field == 'r'
    !doc.containsKey('renamed')
    codec.fromDocument(doc, Annotated) == message.toBuilder().clearSecret().build()
    codec.fromDocument([id: 'a', secret: 'leaked'], Annotated) == Annotated.newBuilder().setId('a').build()
  }

  def "ignored fields inside nested and repeated messages are pruned"() {
    given:
    def overrides = MappingOverrides.builder().ignorePath('parties.email').ignorePath('document.body').build()
    def mapper = ProtoMapper.builder().overrides(overrides).build()
    def pruning = ProtoDocumentCodec.of(mapper)
    def item = InboxItem.newBuilder().setId('in-1').setDocument(document()).build()

    expect:
    pruning.toDocument(document()).parties*.keySet() == [['name', 'role'] as Set, ['name', 'role'] as Set]
    !pruning.toDocument(item).document.containsKey('body')
    pruning.toDocument(item).document.title == 'Motion to dismiss'
  }

  def "unknown document keys are ignored or rejected by policy"() {
    given:
    def strict = ProtoDocumentCodec.of(ProtoMapper.withDefaults(), CodecConfig.standard().withUnknownFields(UnknownFieldPolicy.FAIL))

    expect:
    codec.fromDocument([id: 'x', legacy: true], Document).id == 'x'

    when:
    strict.fromDocument([id: 'x', legacy: true], Document)

    then:
    def e = thrown(CodecException)
    e.message.contains('legacy')

    when:
    strict.fromDocument([parties: [[name: 'n', stale: 1]]], Document)

    then:
    thrown(CodecException)
  }

  static TypedMaps typedMaps() {
    TypedMaps.newBuilder()
        .setId('tm-1')
        .putCounters('views', 15L)
        .putCounters('big', 9007199254740993L)
        .putNotes('summary', 'Quarterly reports are due')
        .putDue('review', FILED)
        .putLevels('east', TypedMapLevel.HIGH)
        .putPlaces('home', LatLon.newBuilder().setLat(36.1d).setLon(-115.1d).build())
        .putChildren('alice', TypedChild.newBuilder().setName('Alice').setCount(3L)
            .setDisplayName('Ally').setSecret('hidden')
            .addItems(TypedItem.newBuilder().setSku('A-1').setQuantity(2)).build())
        .putBags('b', TypedBag.newBuilder().putTags('k', 'v').build())
        .putLabels('env', 'prod')
        .build()
  }

  def "typed maps convert every value by its type and read back"() {
    given:
    def message = typedMaps()

    when:
    def document = codec.toDocument(message)

    then:
    document.counters == [big: 9007199254740993L, views: 15L]
    document.notes == [summary: 'Quarterly reports are due']
    document.due == [review: '2024-03-04T05:06:07.123Z']
    document.levels == [east: 'HIGH']
    document.places == [home: [lat: 36.1d, lon: -115.1d]]
    document.children == [alice: [name: 'Alice', count: 3L, label: 'Ally',
                                  items: [[sku: 'A-1', quantity: 2]]]]
    document.bags == [b: [tags: [k: 'v']]]
    document.labels == [env: 'prod']

    and: 'the ignored field inside a value is pruned and the renamed one restored'
    codec.fromDocument(document, TypedMaps) == message.toBuilder()
        .putChildren('alice', message.getChildrenOrThrow('alice').toBuilder().clearSecret().build())
        .build()
    codec.fromDocument(Json.parseObject(Json.canonical(document)), TypedMaps).getChildrenOrThrow('alice')
        .displayName == 'Ally'
  }

  def "typed map values follow the codec settings"() {
    given:
    def strings = ProtoDocumentCodec.of(ProtoMapper.withDefaults(), CodecConfig.standard().withInt64AsNumber(false))
    def message = TypedMaps.newBuilder().putCounters('views', 5L).putChildren('k', TypedChild.newBuilder().build()).build()

    expect:
    strings.toDocument(message).counters == [views: '5']
    codec.toDocument(message).children == [k: [name: '', count: 0L, label: '', items: []]]
    sparse.toDocument(message).children == [k: [:]]
    codec.fromDocument([counters: [views: 5, other: '6'], due: [d: 1709528767123]], TypedMaps).countersMap ==
        [views: 5L, other: 6L]
    codec.fromDocument([due: [d: 1709528767123]], TypedMaps).getDueOrThrow('d') == FILED
    codec.fromDocument([places: [p: 'POINT (-115.1 36.1)']], TypedMaps).getPlacesOrThrow('p').lat == 36.1d
  }

  @Unroll
  def "a typed map key '#key' is rejected"() {
    when:
    codec.toDocument(TypedMaps.newBuilder().putCounters(key, 1L).build())

    then:
    def e = thrown(CodecException)
    e.message == "Field counters has the key '${key}'; keys of a map mapped as OBJECT must be non-empty and contain no '.'"

    where:
    key << ['a.b', '']
  }

  def "keys of flattened maps stay opaque"() {
    expect:
    codec.toDocument(TypedMaps.newBuilder().putLabels('a.b', 'x').putLabels('', 'y').build()).labels ==
        ['a.b': 'x', '': 'y']
  }

  def "unknown fields inside typed map values are rejected when strict"() {
    given:
    def strict = ProtoDocumentCodec.of(ProtoMapper.withDefaults(), CodecConfig.standard().withUnknownFields(UnknownFieldPolicy.FAIL))

    when:
    strict.fromDocument([children: [k: [name: 'n', stale: 1]]], TypedMaps)

    then:
    def e = thrown(CodecException)
    e.message.contains('children.*.stale')
  }

  def "an unknown enum value is tolerated by default and rejected when strict"() {
    given:
    def strict = ProtoDocumentCodec.of(ProtoMapper.withDefaults(), CodecConfig.standard().withUnknownFields(UnknownFieldPolicy.FAIL))

    expect:
    codec.fromDocument([type: 'REMOVED_TYPE'], Document).type == DocumentType.DOCUMENT_TYPE_UNSPECIFIED

    when:
    strict.fromDocument([type: 'REMOVED_TYPE'], Document)

    then:
    thrown(CodecException)
  }

  def "documents can be read into DynamicMessage by descriptor and from hits"() {
    given:
    def doc = codec.toDocument(firm())

    expect:
    codec.fromDocument(doc, Firm.descriptor).getField(Firm.descriptor.findFieldByName('name')) == firm().name
    codec.fromHit(new Hit('firms-v1', 'firm-1', 1.0d, doc, []), Firm) == firm()
    codec.definition(Firm.descriptor).alias() == 'firms'
  }

  def "the document id comes from the id field"() {
    expect:
    codec.idOf(document()) == Optional.of('doc-1')
    codec.idOf(firm()) == Optional.of('firm-1')
    codec.idOf(Document.getDefaultInstance()) == Optional.empty()
    codec.idOf(Scalars.getDefaultInstance()) == Optional.empty()
    codec.idOf(IdByOption.newBuilder().setKey('k1').build()) == Optional.of('k1')
  }

  def "ids can be numbers, enums or timestamps but not messages or bytes"() {
    given:
    def numericId = ProtoDocumentCodec.of(ProtoMapper.builder()
        .overrides(MappingOverrides.builder().forMessage(Scalars.descriptor) { it.idField = field }.build()).build())

    expect:
    numericId.idOf(message) == expected

    where:
    field          | message                                                 | expected
    'int64_value'  | Scalars.newBuilder().setInt64Value(99L).build()          | Optional.of('99')
    'enum_value'   | Scalars.newBuilder().setEnumValue(Color.BLUE).build()    | Optional.of('BLUE')
    'bool_value'   | Scalars.newBuilder().setBoolValue(true).build()          | Optional.of('true')
  }

  def "a timestamp id is formatted and a nested id walks the message"() {
    given:
    def timestampId = ProtoDocumentCodec.of(ProtoMapper.builder()
        .overrides(MappingOverrides.builder().forMessage(WellKnown.descriptor) { it.idField = 'timestamp_value' }.build()).build())
    def nestedId = ProtoDocumentCodec.of(ProtoMapper.builder()
        .overrides(MappingOverrides.builder().forMessage(InboxItem.descriptor) { it.idField = 'document.id' }.build()).build())

    expect:
    timestampId.idOf(WellKnown.newBuilder().setTimestampValue(FILED).build()) == Optional.of('2024-03-04T05:06:07.123Z')
    nestedId.idOf(InboxItem.newBuilder().setDocument(document()).build()) == Optional.of('doc-1')
    nestedId.idOf(InboxItem.getDefaultInstance()) == Optional.empty()
  }

  def "a bytes or repeated id field is refused"() {
    given:
    def bytesId = ProtoDocumentCodec.of(ProtoMapper.builder()
        .overrides(MappingOverrides.builder().forMessage(Scalars.descriptor) { it.idField = 'bytes_value' }.build()).build())
    def repeatedId = ProtoDocumentCodec.of(ProtoMapper.builder()
        .overrides(MappingOverrides.builder().forMessage(Scalars.descriptor) { it.idField = 'repeated_strings' }.build()).build())

    when:
    bytesId.idOf(Scalars.newBuilder().setBytesValue(ByteString.copyFromUtf8('x')).build())

    then:
    thrown(CodecException)

    when:
    repeatedId.idOf(Scalars.getDefaultInstance())

    then:
    thrown(CodecException)
  }

  def "an explicit definition can be supplied"() {
    given:
    def definition = ProtoMapper.builder()
        .overrides(MappingOverrides.builder().forField(Firm.descriptor, 'domain') { it.name = 'web' }.build())
        .build().map(Firm)

    expect:
    codec.toDocument(firm(), definition).web == 'dch.example'
    !codec.toDocument(firm(), definition).containsKey('domain')
  }

  def "a class without newBuilder is rejected"() {
    when:
    codec.fromDocument([:], com.google.protobuf.DynamicMessage)

    then:
    thrown(IllegalArgumentException)
  }
}
