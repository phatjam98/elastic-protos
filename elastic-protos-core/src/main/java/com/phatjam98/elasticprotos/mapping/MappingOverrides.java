package com.phatjam98.elasticprotos.mapping;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.phatjam98.elasticprotos.proto.EsType;
import com.phatjam98.elasticprotos.proto.FieldOpts;
import com.phatjam98.elasticprotos.proto.MessageOpts;
import com.phatjam98.elasticprotos.util.Json;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * Programmatic mapping overrides for teams that cannot, or would rather not, annotate their protos.
 * Overrides use the same option messages as {@code elastic_options.proto} and win over them; only
 * the fields set in an override replace what the proto declared.
 *
 * <p>Field overrides can be addressed by message and field name, which applies wherever that
 * message appears, or by the dotted path from the root message being mapped.
 */
public final class MappingOverrides {

  private static final MappingOverrides NONE = builder().build();

  private final Map<String, MessageOpts> messages;
  private final Map<String, FieldOpts> fieldsByMessage;
  private final Map<String, FieldOpts> fieldsByPath;
  private final Map<String, Object> settings;

  private MappingOverrides(Builder builder) {
    this.messages = Map.copyOf(builder.messages);
    this.fieldsByMessage = Map.copyOf(builder.fieldsByMessage);
    this.fieldsByPath = Map.copyOf(builder.fieldsByPath);
    this.settings = Collections.unmodifiableMap(Json.sortedCopy(builder.settings));
  }

  /** Returns an empty set of overrides. */
  public static MappingOverrides none() {
    return NONE;
  }

  /** Starts building overrides. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the message-level override for a message, if any.
   *
   * @param descriptor the message
   * @return the override options
   */
  public Optional<MessageOpts> message(Descriptor descriptor) {
    return Optional.ofNullable(messages.get(descriptor.getFullName()));
  }

  /**
   * Returns the field-level override for a field: the by-message override merged with the by-path
   * override, the latter winning.
   *
   * @param field the field
   * @param path the field's dotted path from the root message
   * @return the merged override, or the default instance when none applies
   */
  public FieldOpts field(FieldDescriptor field, String path) {
    FieldOpts byMessage = fieldsByMessage.get(key(field.getContainingType().getFullName(),
        field.getName()));
    FieldOpts byPath = fieldsByPath.get(path);
    if (byMessage == null && byPath == null) {
      return FieldOpts.getDefaultInstance();
    }
    if (byMessage == null) {
      return byPath;
    }
    if (byPath == null) {
      return byMessage;
    }
    return byMessage.toBuilder().mergeFrom(byPath).build();
  }

  /** Returns extra index settings merged over the message's own settings. */
  public Map<String, Object> settings() {
    return settings;
  }

  private static String key(String messageFullName, String fieldName) {
    return messageFullName + "#" + fieldName;
  }

  /** Builder for {@link MappingOverrides}. */
  public static final class Builder {

    private final Map<String, MessageOpts> messages = new HashMap<>();
    private final Map<String, FieldOpts> fieldsByMessage = new HashMap<>();
    private final Map<String, FieldOpts> fieldsByPath = new HashMap<>();
    private final Map<String, Object> settings = new TreeMap<>();

    private Builder() {}

    /** Overrides message-level options for a message. */
    public Builder forMessage(Descriptor descriptor, Consumer<MessageOpts.Builder> configure) {
      return forMessage(descriptor.getFullName(), configure);
    }

    /** Overrides message-level options for a message identified by its proto full name. */
    public Builder forMessage(String messageFullName, Consumer<MessageOpts.Builder> configure) {
      MessageOpts.Builder builder =
          messages.getOrDefault(messageFullName, MessageOpts.getDefaultInstance()).toBuilder();
      configure.accept(builder);
      messages.put(messageFullName, builder.build());
      return this;
    }

    /** Overrides a field wherever its message appears. */
    public Builder forField(
        Descriptor descriptor, String fieldName, Consumer<FieldOpts.Builder> configure) {
      return forField(descriptor.getFullName(), fieldName, configure);
    }

    /** Overrides a field of the message with the given proto full name, wherever it appears. */
    public Builder forField(
        String messageFullName, String fieldName, Consumer<FieldOpts.Builder> configure) {
      String key = key(messageFullName, fieldName);
      FieldOpts.Builder builder =
          fieldsByMessage.getOrDefault(key, FieldOpts.getDefaultInstance()).toBuilder();
      configure.accept(builder);
      fieldsByMessage.put(key, builder.build());
      return this;
    }

    /** Overrides the field at a dotted path from the root message being mapped. */
    public Builder forPath(String path, Consumer<FieldOpts.Builder> configure) {
      FieldOpts.Builder builder =
          fieldsByPath.getOrDefault(path, FieldOpts.getDefaultInstance()).toBuilder();
      configure.accept(builder);
      fieldsByPath.put(path, builder.build());
      return this;
    }

    /** Excludes a field from the mapping and from documents, wherever its message appears. */
    public Builder ignore(String messageFullName, String fieldName) {
      return forField(messageFullName, fieldName, f -> f.setType(EsType.IGNORE));
    }

    /** Excludes the field at a dotted path from the mapping and from documents. */
    public Builder ignorePath(String path) {
      return forPath(path, f -> f.setType(EsType.IGNORE));
    }

    /**
     * Adds an index setting, for example {@code setting("analysis", Map.of(...))} or
     * {@code setting("number_of_shards", 3)}. Nested maps are deep-merged over the message's own
     * settings.
     */
    public Builder setting(String key, Object value) {
      settings.put(key, value);
      return this;
    }

    /** Builds the overrides. */
    public MappingOverrides build() {
      return new MappingOverrides(this);
    }
  }
}
