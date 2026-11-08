package com.phatjam98.elasticprotos.migration;

/**
 * How many replicas a newly created index gets when its settings do not say.
 *
 * @param mode how the count is derived
 * @param count the fixed count, or the upper bound for {@link Mode#DATA_NODES_MINUS_ONE}
 */
public record ReplicaPolicy(Mode mode, int count) {

  /** How the replica count is derived. */
  public enum Mode {
    /** Always {@code count}. */
    FIXED,
    /** The number of data nodes minus one, at least 0 and at most {@code count}. */
    DATA_NODES_MINUS_ONE
  }

  /** Validates the count. */
  public ReplicaPolicy {
    if (mode == null) {
      throw new IllegalArgumentException("mode is required");
    }
    if (count < 0) {
      throw new IllegalArgumentException("replica count must not be negative: " + count);
    }
  }

  /** Always uses {@code replicas} replicas. */
  public static ReplicaPolicy fixed(int replicas) {
    return new ReplicaPolicy(Mode.FIXED, replicas);
  }

  /** Uses one replica fewer than there are data nodes, between 0 and {@code max}. */
  public static ReplicaPolicy dataNodesMinusOne(int max) {
    return new ReplicaPolicy(Mode.DATA_NODES_MINUS_ONE, max);
  }

  /** Returns whether {@link #replicas(int)} needs the data node count. */
  public boolean needsDataNodes() {
    return mode == Mode.DATA_NODES_MINUS_ONE;
  }

  /**
   * Returns the replica count for a cluster.
   *
   * @param dataNodes the number of data nodes; ignored for {@link Mode#FIXED}
   * @return the replica count
   */
  public int replicas(int dataNodes) {
    if (mode == Mode.FIXED) {
      return count;
    }
    return Math.max(0, Math.min(count, dataNodes - 1));
  }
}
