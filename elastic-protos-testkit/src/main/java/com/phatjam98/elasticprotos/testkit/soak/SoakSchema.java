package com.phatjam98.elasticprotos.testkit.soak;

import com.phatjam98.elasticprotos.mapping.IndexNamingStrategy;
import com.phatjam98.elasticprotos.mapping.MappingOverrides;
import com.phatjam98.elasticprotos.mapping.ProtoMapper;
import com.phatjam98.elasticprotos.proto.Bool3;
import com.phatjam98.elasticprotos.proto.EsType;
import com.phatjam98.elasticprotos.proto.example.Document;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 *       so {@code BACKFILL}).
 * </ul>
 */
public final class SoakSchema {

  private static final String[] WORDS = {"merger", "agreement", "motion", "order", "notice",
      "filing", "court", "party", "counsel", "exhibit", "brief", "appeal", "summary", "judgment"};
  private static final String[] TYPES = {"FILING", "ORDER", "MOTION", "NOTICE"};

  private SoakSchema() {}

  /**
   * Returns the mapper of a version.
   *
   * @param version {@code base}, {@code reindex} or {@code backfill}
   * @param prefix the naming prefix
   * @return the mapper
   */
  public static ProtoMapper mapper(String version, String prefix) {
    ProtoMapper.Builder builder = ProtoMapper.builder()
        .naming(IndexNamingStrategy.prefixed(prefix));
    switch (version) {
      case "base":
        break;
      case "reindex":
        builder.overrides(MappingOverrides.builder()
            .forField(Document.getDescriptor(), "title", f -> f.setType(EsType.KEYWORD))
            .build());
        break;
      case "backfill":
        builder.overrides(MappingOverrides.builder()
            .forField(Document.getDescriptor(), "body", f -> f.setKeywordSubfield(Bool3.TRUE))
            .build());
        break;
      default:
        throw new IllegalArgumentException("unknown mapping version " + version);
    }
    return builder.build();
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
