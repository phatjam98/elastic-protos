package com.phatjam98.elasticprotos.testkit;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.google.protobuf.util.Timestamps;
import com.phatjam98.elasticprotos.proto.LatLon;
import com.phatjam98.elasticprotos.proto.example.Address;
import com.phatjam98.elasticprotos.proto.example.Binder;
import com.phatjam98.elasticprotos.proto.example.BinderNote;
import com.phatjam98.elasticprotos.proto.example.Document;
import com.phatjam98.elasticprotos.proto.example.DocumentType;
import com.phatjam98.elasticprotos.proto.example.Firm;
import com.phatjam98.elasticprotos.proto.example.FirmStatus;
import com.phatjam98.elasticprotos.proto.example.InboxItem;
import com.phatjam98.elasticprotos.proto.example.InboxStatus;
import com.phatjam98.elasticprotos.proto.example.Office;
import com.phatjam98.elasticprotos.proto.example.Party;
import com.phatjam98.elasticprotos.proto.example.PartyRole;
import com.phatjam98.elasticprotos.proto.example.Section;
import com.phatjam98.elasticprotos.proto.example.SectionMeta;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * A small, realistic sample set of law firms and legal documents, shared by the engine
 * integration specification and the demo.
 *
 * <p>The documents are built so that specific queries have known answers: only {@code doc-1}
 * contains both "merger" and "agreement"; {@code doc-2} has Acme Corp as defendant and a different
 * party as plaintiff, so a nested query for "Acme Corp as plaintiff" must not match it; only
 * {@code doc-6} has a relevance score and metadata.
 */
public final class SampleData {

  /** Las Vegas, the location of {@code firm-1}'s headquarters. */
  public static final LatLon LAS_VEGAS = latLon(36.1699, -115.1398);

  private SampleData() {}

  /** Returns three firms in Las Vegas, New York and Albuquerque, with nested offices. */
  public static List<Firm> firms() {
    return List.of(
        Firm.newBuilder()
            .setId("firm-1")
            .setName("Dewey Cheatem & Howe LLP")
            .setDomain("dch.example")
            .setStatus(FirmStatus.ACTIVE)
            .setHeadquarters(address("1 Fremont St", "Las Vegas", "NV", "89101", LAS_VEGAS))
            .addOffices(office("Reno office", 12,
                address("50 Virginia St", "Reno", "NV", "89501", latLon(39.5296, -119.8138))))
            .addOffices(office("Henderson office", 30,
                address("240 Water St", "Henderson", "NV", "89015", latLon(36.0395, -114.9817))))
            .putLabels("tier", "gold")
            .setCreatedAt(ts("2019-05-01T09:00:00Z"))
            .build(),
        Firm.newBuilder()
            .setId("firm-2")
            .setName("Pearson Specter Litt")
            .setDomain("psl.example")
            .setStatus(FirmStatus.ACTIVE)
            .setHeadquarters(address("601 Lexington Ave", "New York", "NY", "10022",
                latLon(40.7580, -73.9700)))
            .addOffices(office("Boston office", 45,
                address("1 Federal St", "Boston", "MA", "02110", latLon(42.3554, -71.0565))))
            .setCreatedAt(ts("2011-06-23T09:00:00Z"))
            .build(),
        Firm.newBuilder()
            .setId("firm-3")
            .setName("Hamlin Hamlin & McGill")
            .setDomain("hhm.example")
            .setStatus(FirmStatus.SUSPENDED)
            .setHeadquarters(address("200 Central Ave", "Albuquerque", "NM", "87102",
                latLon(35.0844, -106.6504)))
            .addOffices(office("Santa Fe office", 8,
                address("100 Palace Ave", "Santa Fe", "NM", "87501", latLon(35.6870, -105.9378))))
            .setCreatedAt(ts("2008-01-20T09:00:00Z"))
            .build());
  }

  /** Returns six documents around legal topics, filed between January and June 2024. */
  public static List<Document> documents() {
    return List.of(
        Document.newBuilder()
            .setId("doc-1")
            .setFirmId("firm-1")
            .setTitle("Merger agreement between Acme and Globex")
            .setBody("This merger agreement sets out the terms under which Acme Corp acquires"
                + " Globex Corp, including the exchange ratio and closing conditions.")
            .setType(DocumentType.FILING)
            .addTags("corporate").addTags("m-and-a")
            .addParties(party("Acme Corp", PartyRole.PLAINTIFF, "legal@acme.example"))
            .addParties(party("Globex Corp", PartyRole.DEFENDANT, ""))
            .setFiledAt(ts("2024-01-15T10:30:00.250Z"))
            .setSizeBytes(120_000L)
            .setChecksum(ByteString.copyFromUtf8("sha256:merger"))
            .build(),
        Document.newBuilder()
            .setId("doc-2")
            .setFirmId("firm-2")
            .setTitle("Motion to dismiss")
            .setBody("The defendant moves to dismiss the complaint for failure to state a claim"
                + " upon which relief can be granted.")
            .setType(DocumentType.MOTION)
            .addTags("civil").addTags("urgent")
            .addParties(party("Jane Doe", PartyRole.PLAINTIFF, "jane@doe.example"))
            .addParties(party("Acme Corp", PartyRole.DEFENDANT, "legal@acme.example"))
            .setFiledAt(ts("2024-02-20T14:00:00Z"))
            .setSizeBytes(45_000L)
            .build(),
        Document.newBuilder()
            .setId("doc-3")
            .setFirmId("firm-2")
            .setTitle("Order granting summary judgment")
            .setBody("The court grants summary judgment in favor of the plaintiff on all"
                + " counts.")
            .setType(DocumentType.ORDER)
            .addTags("civil")
            .addParties(party("Initech LLC", PartyRole.PLAINTIFF, ""))
            .addParties(party("Globex Corp", PartyRole.DEFENDANT, ""))
            .setFiledAt(ts("2024-03-10T09:15:00Z"))
            .setSizeBytes(30_000L)
            .build(),
        Document.newBuilder()
            .setId("doc-4")
            .setFirmId("firm-3")
            .setTitle("Notice of appearance")
            .setBody("Counsel enters an appearance on behalf of the defendant.")
            .setType(DocumentType.NOTICE)
            .addTags("admin")
            .addParties(party("Saul Goodman", PartyRole.COUNSEL, "saul@hhm.example"))
            .setFiledAt(ts("2024-04-05T16:45:00Z"))
            .setSizeBytes(8_000L)
            .build(),
        Document.newBuilder()
            .setId("doc-5")
            .setFirmId("firm-1")
            .setTitle("Asset purchase agreement")
            .setBody("Seller transfers the listed assets to the buyer at closing.")
            .setType(DocumentType.FILING)
            .addTags("corporate")
            .setFiledAt(ts("2024-05-12T11:00:00Z"))
            .setSizeBytes(250_000L)
            .build(),
        Document.newBuilder()
            .setId("doc-6")
            .setFirmId("firm-3")
            .setTitle("Deposition transcript")
            .setBody("Transcript of the deposition of the witness, taken in Las Vegas.")
            .setType(DocumentType.FILING)
            .addTags("discovery")
            .addParties(party("John Roe", PartyRole.DEFENDANT, ""))
            .setFiledAt(ts("2024-06-30T08:00:00Z"))
            .setSizeBytes(2_500_000L)
            .setMetadata(Struct.newBuilder()
                .putFields("pages", Value.newBuilder().setNumberValue(120).build())
                .putFields("court_reporter", Value.newBuilder().setStringValue("A. Smith").build()))
            .setInternalNotes("privileged: do not index")
            .setRelevance(0.75d)
            .build());
  }

  /**
   * Returns {@code count} small inbox items for paging tests, with ids {@code inbox-00000} upward
   * so that sorting by id is the natural order.
   */
  public static List<InboxItem> inboxItems(int count) {
    List<InboxItem> items = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      items.add(InboxItem.newBuilder()
          .setId(String.format("inbox-%05d", i))
          .setFirmId("firm-" + (i % 3 + 1))
          .setSubject("Notification " + i)
          .setStatus(i % 2 == 0 ? InboxStatus.UNREAD : InboxStatus.READ)
          .setReceivedAt(Timestamp.newBuilder().setSeconds(1_704_067_200L + i * 60L))
          .addRecipientEmails("clerk" + (i % 7) + "@court.example")
          .build());
    }
    return items;
  }

  /**
   * Returns two binders for nested highlighting. {@code binder-1} has three sections, each with
   * notes about "red" (two in the first), and an annotation "red ink" under the first section's
   * meta; {@code binder-2} mentions "red" only in a heading.
   */
  public static List<Binder> binders() {
    return List.of(
        Binder.newBuilder()
            .setId("binder-1")
            .setTitle("Field notes")
            .addSections(Section.newBuilder().setHeading("red section")
                .addNotes(note("red apple")).addNotes(note("blue sky")).addNotes(note("red fox"))
                .setMeta(SectionMeta.newBuilder().addAnnotations(note("blue pen"))
                    .addAnnotations(note("red ink"))))
            .addSections(Section.newBuilder().setHeading("blue section")
                .addNotes(note("red sun")))
            .addSections(Section.newBuilder().setHeading("green section")
                .addNotes(note("red rose")))
            .build(),
        Binder.newBuilder()
            .setId("binder-2")
            .setTitle("Plain binder")
            .addSections(Section.newBuilder().setHeading("red heading").addNotes(note("plain")))
            .build());
  }

  private static BinderNote note(String text) {
    return BinderNote.newBuilder().setText(text).build();
  }

  /** Returns a lat/lon point. */
  public static LatLon latLon(double lat, double lon) {
    return LatLon.newBuilder().setLat(lat).setLon(lon).build();
  }

  /** Parses an RFC 3339 timestamp. */
  public static Timestamp ts(String rfc3339) {
    try {
      return Timestamps.parse(rfc3339);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Not an RFC 3339 timestamp: " + rfc3339, e);
    }
  }

  private static Address address(String street, String city, String state, String postalCode,
      LatLon location) {
    return Address.newBuilder().setStreet(street).setCity(city).setState(state)
        .setPostalCode(postalCode).setLocation(location).build();
  }

  private static Office office(String name, int headcount, Address address) {
    return Office.newBuilder().setName(name).setHeadcount(headcount).setAddress(address).build();
  }

  private static Party party(String name, PartyRole role, String email) {
    return Party.newBuilder().setName(name).setRole(role).setEmail(email).build();
  }
}
