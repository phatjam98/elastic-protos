package com.acme.search;

import com.acme.search.proto.Document;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.phatjam98.elasticprotos.engine.BulkBuffer;
import com.phatjam98.elasticprotos.engine.Hit;
import com.phatjam98.elasticprotos.engine.IndexOptions;
import com.phatjam98.elasticprotos.engine.IndexResult;
import com.phatjam98.elasticprotos.micronaut.ElasticProtosService;
import com.phatjam98.elasticprotos.micronaut.SearchPage;
import com.phatjam98.elasticprotos.query.Criteria;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** CRUD and full-text search over {@link Document}s stored behind the "documents" alias. */
@Controller("/documents")
public class DocumentController {

  // Waits until the write is searchable, so a search right after a POST finds it. Use
  // IndexOptions.defaults() for throughput when read-your-writes does not matter.
  private static final IndexOptions VISIBLE =
      new IndexOptions(false, IndexOptions.Refresh.WAIT_FOR);

  private final ElasticProtosService elastic;
  private final DocumentJson json;

  /**
   * Creates the controller.
   *
   * @param elastic the elastic-protos service
   * @param json the JSON converter
   */
  public DocumentController(ElasticProtosService elastic, DocumentJson json) {
    this.elastic = elastic;
    this.json = json;
  }

  /** Response of a write: the document id and the engine's result. */
  public record IndexResponse(String id, String result) {}

  /**
   * Response of a bulk write: how many documents were sent, how many bulk requests carried them,
   * how many were indexed, and why the others failed.
   */
  public record BulkResponse(int submitted, int batches, int indexed, List<String> errors) {}

  /** One search hit: the id, the score and the document as protobuf JSON. */
  public record SearchHit(String id, Double score, JsonNode document) {}

  /** One page of search results. */
  public record SearchResponse(long total, boolean totalExact, List<SearchHit> hits,
      List<String> warnings) {}

  /**
   * Indexes one document; the id is generated when the JSON has none.
   *
   * @param body the document as protobuf JSON
   * @return 201 with the id
   * @throws InvalidProtocolBufferException when the JSON does not fit {@link Document}
   */
  @Post
  @Consumes(MediaType.APPLICATION_JSON)
  public HttpResponse<IndexResponse> create(@Body String body)
      throws InvalidProtocolBufferException {
    IndexResult result = elastic.index(json.parse(body), VISIBLE);
    return HttpResponse.created(new IndexResponse(result.id(), result.result()));
  }

  /**
   * Indexes a JSON array of documents through a bulk buffer.
   *
   * @param body the documents as a JSON array
   * @return 202 with the number of documents sent, batches, indexed items and errors
   * @throws InvalidProtocolBufferException when an element does not fit {@link Document}
   */
  @Post("/_bulk")
  @Consumes(MediaType.APPLICATION_JSON)
  public HttpResponse<BulkResponse> bulk(@Body String body) throws InvalidProtocolBufferException {
    List<Document> documents = json.parseArray(body);
    AtomicInteger batches = new AtomicInteger();
    AtomicInteger indexed = new AtomicInteger();
    List<String> errors = new CopyOnWriteArrayList<>();
    // The buffer sends batches as they fill (elastic-protos.bulk.*) and the rest on close; the
    // listeners see every batch result and every failed bulk request.
    try (BulkBuffer buffer = elastic.bulkBuffer(
        result -> {
          batches.incrementAndGet();
          indexed.addAndGet(result.items().size() - result.failures().size());
          result.failures().forEach(item -> errors.add(item.id() + ": " + item.error()));
        },
        error -> {
          batches.incrementAndGet();
          errors.add(String.valueOf(error.getMessage()));
        })) {
      documents.forEach(document -> buffer.add(elastic.indexOp(document)));
    }
    // Bulk writes are not refreshed; make them searchable before answering.
    elastic.engine().refresh(elastic.definition(Document.class).alias());
    return HttpResponse.accepted().body(
        new BulkResponse(documents.size(), batches.get(), indexed.get(), List.copyOf(errors)));
  }

  /**
   * Full-text search across every TEXT field (title, body, parties.name).
   *
   * @param q the text to search for
   * @param size the page size; 0 uses {@code elastic-protos.query.default-size}
   * @param from the offset of the first hit
   * @return the page
   */
  @Get("/search")
  public SearchResponse search(@QueryValue String q, @QueryValue(defaultValue = "0") int size,
      @QueryValue(defaultValue = "0") int from) {
    SearchPage<Document> page = elastic.search(
        Criteria.builder().fullText(q).page(size, from).build(), Document.class);
    List<SearchHit> hits = new ArrayList<>(page.items().size());
    for (int i = 0; i < page.items().size(); i++) {
      Hit hit = page.hits().get(i);
      hits.add(new SearchHit(hit.id(), hit.score(), json.toTree(page.items().get(i))));
    }
    return new SearchResponse(page.total(), page.totalExact(), hits, page.warnings());
  }

  /**
   * Reads one document.
   *
   * @param id the document id
   * @return the document as protobuf JSON, or 404
   */
  @Get("/{id}")
  public HttpResponse<JsonNode> get(String id) {
    return elastic.get(id, Document.class)
        .map(document -> HttpResponse.ok(json.toTree(document)))
        .orElseGet(HttpResponse::notFound);
  }

  /**
   * Deletes one document.
   *
   * @param id the document id
   * @return 204, or 404 when there was no such document
   */
  @Delete("/{id}")
  public HttpResponse<Void> delete(String id) {
    // GET by id is realtime either way; the refresh also drops the document from searches
    // before the 204.
    return elastic.delete(id, Document.class, IndexOptions.refreshed())
        ? HttpResponse.noContent()
        : HttpResponse.notFound();
  }

  /**
   * Turns JSON that does not fit the message into a 400.
   *
   * @param request the request
   * @param e the parse failure
   * @return 400 with the message
   */
  @Error(exception = InvalidProtocolBufferException.class)
  public HttpResponse<Map<String, String>> badJson(HttpRequest<?> request,
      InvalidProtocolBufferException e) {
    return HttpResponse.badRequest(Map.of("error", String.valueOf(e.getMessage())));
  }
}
