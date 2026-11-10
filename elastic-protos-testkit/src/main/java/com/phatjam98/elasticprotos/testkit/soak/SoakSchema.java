package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.mapping.IndexNamingStrategy;
import com.phatjam98.elasticprotos.mapping.MappingOverrides;
import com.phatjam98.elasticprotos.mapping.ProtoMapper;
import com.phatjam98.elasticprotos.migration.MigrationScriptProvider;
import com.phatjam98.elasticprotos.proto.Bool3;
import com.phatjam98.elasticprotos.proto.EsType;
import com.phatjam98.elasticprotos.proto.example.Document;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

/**
 * The mappings and documents of the soak runs: the example {@code Document} under a naming prefix,
 * in three mapping versions.
 *
 * <ul>
 *   <li>{@code base}: the proto as it is;
 *   <li>{@code reindex}: {@code title} becomes a {@code keyword} (a type change, so {@code
 *       REINDEX});
 *   <li>{@code backfill}: {@code body} gains a {@code keyword} multi-field (an unindexed addition,
 *       so {@code BACKFILL});
 *   <li>{@code scripted}: the {@code reindex} mapping, migrated through {@link #SCRIPT}, a
 *       content-preserving migration script, so the staging-index reconcile and the probe run.
 * </ul>
 *
 * <p>Every version carries the same {@code number_of_shards} setting (outside the content hash).
 */
public final class SoakSchema {

  private static final String[] WORDS = {"merger", "agreement", "motion", "order", "notice",
      "filing", "court", "party", "counsel", "exhibit", "brief", "appeal", "summary", "judgment"};
  private static final String[] TYPES = {"FILING", "ORDER", "MOTION", "NOTICE"};

  /** The migration script of the {@code scripted} version: changes nothing in the documents. */
  public static final String SCRIPT = "ctx._source.remove('soak_no_such_field')";

  private SoakSchema() {}

  /**
   * Returns the mapper of a version.
   *
   * @param version {@code base}, {@code reindex}, {@code backfill} or {@code scripted}
   * @param prefix the naming prefix
   * @param shards the number of shards of every index
   * @return the mapper
   */
  public static ProtoMapper mapper(String version, String prefix, int shards) {
    MappingOverrides.Builder overrides = MappingOverrides.builder()
        .setting("number_of_shards", shards);
    switch (version) {
      case "base":
        break;
      case "reindex":
      case "scripted":
        overrides.forField(Document.getDescriptor(), "title", f -> f.setType(EsType.KEYWORD));
        break;
      case "backfill":
        overrides.forField(Document.getDescriptor(), "body",
            f -> f.setKeywordSubfield(Bool3.TRUE));
        break;
      default:
        throw new IllegalArgumentException("unknown mapping version " + version);
    }
    return ProtoMapper.builder().naming(IndexNamingStrategy.prefixed(prefix))
        .overrides(overrides.build()).build();
  }

  /** Returns the script provider of a version: {@link #SCRIPT} for {@code scripted}. */
  public static MigrationScriptProvider scripts(String version) {
    return "scripted".equals(version)
        ? (alias, from, to) -> Optional.of(SCRIPT) : MigrationScriptProvider.none();
  }

  /**
   * Checks that the target mapping of a version is in effect on an index.
   *
   * @return null when it is, otherwise what is wrong
   */
  public static String mappingProblem(Map<String, Object> mappings, String version) {
    Map<String, Object> properties = Cluster.map(mappings.get("properties"));
    switch (version) {
      case "reindex":
      case "scripted":
        Object type = Cluster.map(properties.get("title")).get("type");
        return "keyword".equals(type) ? null : "title is mapped as " + type + ", not keyword";
      case "backfill":
        Object keyword = Cluster.map(Cluster.map(properties.get("body")).get("fields"))
            .get("keyword");
        return keyword != null ? null : "body has no keyword multi-field";
      default:
        return null;
    }
  }

  /**
   * Returns a full document.
   *
   * @param id the id
   * @param rev a value unique to this write, stored in {@code size_bytes}
   * @param random the source of variety
   * @return the document
   */
  public static Map<String, Object> document(String id, long rev, Random random) {
    Map<String, Object> doc = new LinkedHashMap<>();
    doc.put("id", id);
    doc.put("firm_id", "firm-" + random.nextInt(50));
    doc.put("title", words(random, 3) + " " + rev);
    doc.put("body", words(random, 12));
    doc.put("type", TYPES[random.nextInt(TYPES.length)]);
    doc.put("tags", List.of(WORDS[random.nextInt(WORDS.length)],
        WORDS[random.nextInt(WORDS.length)]));
    doc.put("size_bytes", rev);
    return doc;
  }

  /**
   * Returns a partial document for {@code _update}: {@code size_bytes} and one or two other
   * fields.
   *
   * @param rev a value unique to this write
   * @param random the source of variety
   * @return the partial document
   */
  public static Map<String, Object> partial(long rev, Random random) {
    Map<String, Object> doc = new LinkedHashMap<>();
    doc.put("size_bytes", rev);
    switch (random.nextInt(3)) {
      case 0:
        doc.put("title", words(random, 2) + " updated " + rev);
        break;
      case 1:
        doc.put("tags", List.of("updated", WORDS[random.nextInt(WORDS.length)]));
        break;
      default:
        doc.put("firm_id", "firm-" + random.nextInt(50));
        doc.put("type", TYPES[random.nextInt(TYPES.length)]);
        break;
    }
    return doc;
  }

  private static String words(Random random, int count) {
    StringBuilder text = new StringBuilder();
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        text.append(' ');
      }
      text.append(WORDS[random.nextInt(WORDS.length)]);
    }
    return text.toString();
  }
}
