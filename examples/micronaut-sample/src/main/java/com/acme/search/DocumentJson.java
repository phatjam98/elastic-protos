package com.acme.search;

import com.acme.search.proto.Document;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts between HTTP JSON and {@link Document} messages with the protobuf JSON mapping
 * ({@code firmId} or {@code firm_id}, enums by name, timestamps as RFC 3339 strings).
 */
@Singleton
public class DocumentJson {

  private final ObjectMapper mapper;
  private final JsonFormat.Parser parser = JsonFormat.parser();
  private final JsonFormat.Printer printer = JsonFormat.printer();

  /**
   * Creates the converter.
   *
   * @param mapper the Jackson mapper used to embed protobuf JSON in responses
   */
  public DocumentJson(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  /**
   * Parses one document.
   *
   * @param json a JSON object
   * @return the document
   * @throws InvalidProtocolBufferException when the JSON does not fit the message
   */
  public Document parse(String json) throws InvalidProtocolBufferException {
    Document.Builder builder = Document.newBuilder();
    parser.merge(json, builder);
    return builder.build();
  }

  /**
   * Parses a JSON array of documents.
   *
   * @param json a JSON array of objects
   * @return the documents, in order
   * @throws InvalidProtocolBufferException when the JSON is not an array or an element does not
   *     fit the message
   */
  public List<Document> parseArray(String json) throws InvalidProtocolBufferException {
    JsonNode array;
    try {
      array = mapper.readTree(json);
    } catch (JsonProcessingException e) {
      throw new InvalidProtocolBufferException("Malformed JSON: " + e.getOriginalMessage());
    }
    if (array == null || !array.isArray()) {
      throw new InvalidProtocolBufferException("Expected a JSON array of documents");
    }
    List<Document> documents = new ArrayList<>(array.size());
    for (JsonNode element : array) {
      documents.add(parse(element.toString()));
    }
    return documents;
  }

  /**
   * Prints a message as protobuf JSON.
   *
   * @param message the message
   * @return the JSON text
   */
  public String print(Message message) {
    try {
      return printer.print(message);
    } catch (InvalidProtocolBufferException e) {
      // Only thrown for Any fields with unknown types, which these messages do not have.
      throw new IllegalStateException(e);
    }
  }

  /**
   * Prints a message as a JSON tree, so it can be embedded in a response object.
   *
   * @param message the message
   * @return the JSON tree
   */
  public JsonNode toTree(Message message) {
    try {
      return mapper.readTree(print(message));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }
}
