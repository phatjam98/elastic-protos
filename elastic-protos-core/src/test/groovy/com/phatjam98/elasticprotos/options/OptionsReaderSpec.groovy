package com.phatjam98.elasticprotos.options

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.ElasticOptionsProto
import com.phatjam98.elasticprotos.proto.EsType
import com.phatjam98.elasticprotos.proto.FieldOpts
import com.phatjam98.elasticprotos.proto.MessageOpts
import com.phatjam98.elasticprotos.proto.test.Annotated
import com.phatjam98.elasticprotos.proto.test.Scalars
import spock.lang.Specification

class OptionsReaderSpec extends Specification {

  def "reads options declared in the proto"() {
    expect:
    OptionsReader.messageOpts(Annotated.descriptor).alias == 'annotated'
    OptionsReader.messageOpts(Annotated.descriptor).shards == 2
    OptionsReader.fieldOpts(Annotated.descriptor.findFieldByName('title')).type == EsType.TEXT
    OptionsReader.fieldOpts(Annotated.descriptor.findFieldByName('title')).boost == 2.5f
  }

  def "returns default instances when nothing is declared"() {
    expect:
    OptionsReader.messageOpts(Scalars.descriptor) == MessageOpts.defaultInstance
    OptionsReader.fieldOpts(Scalars.descriptor.findFieldByName('string_value')) == FieldOpts.defaultInstance
  }

  def "reads options that survived only as unknown fields, as in descriptors built at runtime"() {
    given: 'the Annotated file descriptor rebuilt without the extension registry'
    def file = Annotated.descriptor.file
    def proto = DescriptorProtos.FileDescriptorProto.parseFrom(file.toProto().toByteArray())
    def rebuilt = Descriptors.FileDescriptor.buildFrom(proto, file.dependencies as Descriptors.FileDescriptor[])
    def descriptor = rebuilt.findMessageTypeByName('Annotated')

    expect: 'the extension is indeed unknown on the rebuilt descriptor'
    !descriptor.options.hasExtension(ElasticOptionsProto.elastic)
    !descriptor.findFieldByName('title').options.hasExtension(ElasticOptionsProto.elasticField)

    and: 'the reader still sees the values'
    OptionsReader.messageOpts(descriptor).alias == 'annotated'
    OptionsReader.fieldOpts(descriptor.findFieldByName('title')).boost == 2.5f
    OptionsReader.fieldOpts(descriptor.findFieldByName('secret')).type == EsType.IGNORE

    and: 'the mapper produces the same index definition as for the generated class'
    ProtoMapper.withDefaults().map(descriptor).contentHash() == ProtoMapper.withDefaults().map(Annotated).contentHash()
  }
}
