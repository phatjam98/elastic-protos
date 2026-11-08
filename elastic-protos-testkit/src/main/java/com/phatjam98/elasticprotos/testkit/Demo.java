package com.phatjam98.elasticprotos.testkit;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.phatjam98.elasticprotos.codec.ProtoDocumentCodec;
import com.phatjam98.elasticprotos.elasticsearch.ElasticsearchEngine;
import com.phatjam98.elasticprotos.engine.BulkOp;
import com.phatjam98.elasticprotos.engine.BulkResult;
import com.phatjam98.elasticprotos.engine.Hit;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.engine.SearchResult;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.mapping.ProtoMapper;
import com.phatjam98.elasticprotos.migration.IndexMigrator;
import com.phatjam98.elasticprotos.migration.MigrationPlan;
import com.phatjam98.elasticprotos.migration.MigrationResult;
import com.phatjam98.elasticprotos.opensearch.OpenSearchEngine;
import com.phatjam98.elasticprotos.proto.SearchCriteria;
import com.phatjam98.elasticprotos.proto.example.Document;
import com.phatjam98.elasticprotos.proto.example.Firm;
import com.phatjam98.elasticprotos.query.CompiledSearch;
import com.phatjam98.elasticprotos.query.Criteria;
import com.phatjam98.elasticprotos.query.QueryCompiler;
import com.phatjam98.elasticprotos.util.Json;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * End-to-end demo against a running cluster: bootstraps the Firm and Document indices,
 * bulk-indexes {@link SampleData}, runs a full-text search for "merger agreement" and parses the
 * best hit back into a {@link Document}.
 *
 * <pre>{@code
 * ./gradlew :elastic-protos-testkit:demo --args="elasticsearch http://localhost:9200"
 * ./gradlew :elastic-protos-testkit:demo --args="opensearch http://localhost:9200"
 * }</pre>
 *
 * <p>Running it again is safe: the second bootstrap is a no-op and documents are overwritten by id.
 */
public final class Demo {

  private Demo() {}

  /**
   * Runs the demo.
   *
   * @param args {@code <elasticsearch|opensearch> <url>}
   * @throws Exception when the cluster cannot be reached or a step fails
   */
  public static void main(String[] args) throws Exception {
    PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8);
    if (args.length != 2) {
      out.println("usage: Demo <elasticsearch|opensearch> <url>");
      System.exit(2);
      return;
    }
    try (SearchEngine engine = connect(args[0], args[1])) {
      run(engine, out);
    }
  }

  /**
   * Runs the demo on an engine and prints each step.
   *
   * @param engine the engine to use
   * @param out where to print
   * @throws Exception when a step fails
   */
  public static void run(SearchEngine engine, PrintStream out) throws Exception {
    out.println("Connected to " + engine.info());

    ProtoMapper mapper = ProtoMapper.withDefaults();
    IndexMigrator migrator = IndexMigrator.builder().engine(engine).mapper(mapper).build();
    MigrationPlan plan = migrator.plan(Firm.class, Document.class);
    out.println();
    out.print(plan.toReport());
    MigrationResult result = migrator.apply(plan);
    if (!result.ok()) {
      throw new IllegalStateException("bootstrap failed: " + result.problems());
    }

    ProtoDocumentCodec codec = ProtoDocumentCodec.of(mapper);
    IndexDefinition firms = mapper.map(Firm.class);
    IndexDefinition documents = mapper.map(Document.class);
    List<BulkOp> ops = new ArrayList<>();
    for (Firm firm : SampleData.firms()) {
      ops.add(op(codec, firms, firm));
    }
    for (Document document : SampleData.documents()) {
      ops.add(op(codec, documents, document));
    }
    BulkResult bulk = engine.bulk(ops);
    if (bulk.errors()) {
      throw new IllegalStateException("bulk indexing failed: " + bulk.failures());
    }
    engine.refresh(firms.alias());
    engine.refresh(documents.alias());
    out.println();
    out.println("Indexed " + bulk.items().size() + " documents in " + bulk.tookMs() + " ms");

    SearchCriteria criteria = Criteria.builder().fullText("merger agreement").build();
    CompiledSearch compiled = QueryCompiler.withDefaults().compile(criteria, documents);
    out.println();
    out.println("FULL_TEXT \"merger agreement\" compiles to:");
    out.println(Json.pretty(compiled.body()));

    SearchResult hits = engine.search(List.of(documents.alias()), compiled.body());
    out.println();
    out.println(hits.total() + " hits:");
    for (Hit hit : hits.hits()) {
      out.println(String.format(Locale.ROOT, "  %7.3f  %-6s %s", hit.score(), hit.id(),
          hit.source().get("title")));
    }
    if (!hits.hits().isEmpty()) {
      Document best = codec.fromHit(hits.hits().get(0), Document.class);
      out.println();
      out.println("Best hit parsed back into a Document:");
      out.println(JsonFormat.printer().print(best));
    }
  }

  private static BulkOp op(ProtoDocumentCodec codec, IndexDefinition index, Message message) {
    String id = codec.idOf(message).orElseThrow();
    return BulkOp.index(index.alias(), id, codec.toDocument(message));
  }

  private static SearchEngine connect(String product, String url) {
    switch (product.toLowerCase(Locale.ROOT)) {
      case "elasticsearch":
        return ElasticsearchEngine.builder().hosts(url).build();
      case "opensearch":
        return OpenSearchEngine.builder().hosts(url).build();
      default:
        throw new IllegalArgumentException(
            "unknown engine " + product + "; expected elasticsearch or opensearch");
    }
  }
}
