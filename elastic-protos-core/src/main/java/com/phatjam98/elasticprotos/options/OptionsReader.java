package com.phatjam98.elasticprotos.options;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import com.phatjam98.elasticprotos.proto.ElasticOptionsProto;
import com.phatjam98.elasticprotos.proto.FieldOpts;
import com.phatjam98.elasticprotos.proto.MessageOpts;

/**
 * Reads the {@code elastic} and {@code elastic_field} options off descriptors. Works both for
 * generated classes, where the extensions are registered, and for descriptors built at runtime
 * from descriptor sets, where the options survive only as unknown fields.
 */
public final class OptionsReader {

  private OptionsReader() {}

  /**
   * Returns the message-level options, or the default instance when none are declared.
   *
   * @param descriptor a message descriptor
   * @return the options, never null
   */
  public static MessageOpts messageOpts(Descriptor descriptor) {
    var options = descriptor.getOptions();
    if (options.hasExtension(ElasticOptionsProto.elastic)) {
      return options.getExtension(ElasticOptionsProto.elastic);
    }
    return fromUnknownFields(
        options.getUnknownFields(), ElasticOptionsProto.elastic, MessageOpts.getDefaultInstance());
  }

  /**
   * Returns the field-level options, or the default instance when none are declared.
   *
   * @param field a field descriptor
   * @return the options, never null
   */
  public static FieldOpts fieldOpts(FieldDescriptor field) {
    var options = field.getOptions();
    if (options.hasExtension(ElasticOptionsProto.elasticField)) {
      return options.getExtension(ElasticOptionsProto.elasticField);
    }
    return fromUnknownFields(
        options.getUnknownFields(),
        ElasticOptionsProto.elasticField,
        FieldOpts.getDefaultInstance());
  }

  @SuppressWarnings("unchecked")
  private static <T extends Message> T fromUnknownFields(
      UnknownFieldSet unknown, ExtensionLite<?, T> extension, T defaultInstance) {
    UnknownFieldSet.Field field = unknown.getField(extension.getNumber());
    if (field.getLengthDelimitedList().isEmpty()) {
      return defaultInstance;
    }
    try {
      Message.Builder builder = defaultInstance.newBuilderForType();
      for (ByteString bytes : field.getLengthDelimitedList()) {
        builder.mergeFrom(bytes);
      }
      return (T) builder.build();
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "Malformed elastic-protos option on " + defaultInstance.getDescriptorForType().getName(),
          e);
    }
  }
}
