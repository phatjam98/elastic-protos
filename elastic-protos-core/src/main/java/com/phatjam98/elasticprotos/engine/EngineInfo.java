package com.phatjam98.elasticprotos.engine;

/**
 * Identity and capabilities of the cluster an engine talks to.
 *
 * @param product the engine family
 * @param version the server version, for example {@code 9.1.0}
 * @param clusterName the cluster name
 */
public record EngineInfo(EngineProduct product, String version, String clusterName) {

  /** Returns whether point-in-time searches are available. */
  public boolean supportsPointInTime() {
    switch (product) {
      case ELASTICSEARCH:
        return atLeast(7, 10);
      case OPENSEARCH:
        return atLeast(2, 4);
      default:
        return false;
    }
  }

  /**
   * Returns whether {@code _shard_doc} can be used as the tiebreaker sort with a point in time:
   * Elasticsearch 7.12+ and OpenSearch 3.3+ (verified: absent in OpenSearch 2.19 and 3.2).
   */
  public boolean supportsShardDoc() {
    switch (product) {
      case ELASTICSEARCH:
        return atLeast(7, 12);
      case OPENSEARCH:
        return atLeast(3, 3);
      default:
        return false;
    }
  }

  /** Returns the major version, or 0 when unknown. */
  public int major() {
    return part(0);
  }

  /** Returns the minor version, or 0 when unknown. */
  public int minor() {
    return part(1);
  }

  private boolean atLeast(int major, int minor) {
    return major() > major || (major() == major && minor() >= minor);
  }

  private int part(int index) {
    String[] parts = version == null ? new String[0] : version.split("[.\\-]");
    if (parts.length <= index) {
      return 0;
    }
    try {
      return Integer.parseInt(parts[index]);
    } catch (NumberFormatException e) {
      return 0;
    }
  }
}
