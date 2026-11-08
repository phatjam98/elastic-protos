package com.acme.search;

import com.acme.search.proto.Document;
import com.acme.search.proto.DocumentType;
import com.acme.search.proto.Party;
import com.acme.search.proto.PartyRole;
import com.google.protobuf.Timestamp;
import com.phatjam98.elasticprotos.engine.IndexOptions;
import com.phatjam98.elasticprotos.micronaut.ElasticProtosService;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.inject.Singleton;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Indexes three sample documents once the server is up, when {@code seed.enabled} is true. The
 * elastic-protos bootstrap has already created the indices by then (it runs on StartupEvent).
 */
@Singleton
@Requires(property = "seed.enabled", value = "true")
public class SampleSeeder implements ApplicationEventListener<ServerStartupEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(SampleSeeder.class);

  private final ElasticProtosService elastic;

  /**
   * Creates the seeder.
   *
   * @param elastic the elastic-protos service
   */
  public SampleSeeder(ElasticProtosService elastic) {
    this.elastic = elastic;
  }

  @Override
  public void onApplicationEvent(ServerStartupEvent event) {
    List<Document> samples = samples();
    for (Document document : samples) {
      elastic.index(document, IndexOptions.refreshed());
    }
    LOG.info("Seeded {} sample documents into alias '{}'", samples.size(),
        elastic.definition(Document.class).alias());
  }

  /** Returns the sample documents. */
  static List<Document> samples() {
    return List.of(
        Document.newBuilder()
            .setId("doc-1")
            .setFirmId("firm-1")
            .setTitle("Merger agreement between Acme and Globex")
            .setBody("This agreement sets out the terms of the merger of Acme Corp into Globex.")
            .setType(DocumentType.FILING)
            .addTags("m-and-a")
            .addParties(party("Acme Corp", PartyRole.PLAINTIFF))
            .addParties(party("Globex Inc", PartyRole.DEFENDANT))
            .setFiledAt(timestamp("2026-03-02T15:04:05Z"))
            .setSizeBytes(48_213)
            .setInternalNotes("never indexed")
            .build(),
        Document.newBuilder()
            .setId("doc-2")
            .setFirmId("firm-1")
            .setTitle("Order granting motion to dismiss")
            .setBody("The court grants the defendant's motion to dismiss for lack of jurisdiction.")
            .setType(DocumentType.ORDER)
            .addTags("litigation")
            .addParties(party("Initech LLC", PartyRole.DEFENDANT))
            .setFiledAt(timestamp("2026-04-11T09:30:00Z"))
            .setSizeBytes(12_904)
            .build(),
        Document.newBuilder()
            .setId("doc-3")
            .setFirmId("firm-2")
            .setTitle("Notice of hearing on the proposed merger")
            .setBody("A hearing on the proposed merger agreement is set for May 5.")
            .setType(DocumentType.NOTICE)
            .addTags("m-and-a")
            .addTags("hearing")
            .addParties(party("Jane Roe", PartyRole.COUNSEL))
            .setFiledAt(timestamp("2026-04-20T13:00:00Z"))
            .setSizeBytes(3_377)
            .build());
  }

  private static Party party(String name, PartyRole role) {
    return Party.newBuilder().setName(name).setRole(role).build();
  }

  private static Timestamp timestamp(String instant) {
    Instant parsed = Instant.parse(instant);
    return Timestamp.newBuilder().setSeconds(parsed.getEpochSecond()).setNanos(parsed.getNano())
        .build();
  }
}
