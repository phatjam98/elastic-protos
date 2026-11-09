package com.phatjam98.elasticprotos.migration;

import java.util.Objects;

/**
 * How many replicas a newly created index gets when its settings do not say. Create one with
 * {@link #fixed(int)} or {@link #dataNodesMinusOne(int)}.
 */
public final class ReplicaPolicy {

  /** How the replica count is derived. */
  public enum Mode {
    /** Always {@code count}. */
    FIXED,
    /** The number of data nodes minus one, at least 0 and at most {@code count}. */
    DATA_NODES_MINUS_ONE
  }

  private final Mode mode;
  private final int count;

  private ReplicaPolicy(Mode mode, int count) {
    if (count < 0) {
      throw new IllegalArgumentException("replica count must not be negative: " + count);
    }
    this.mode = mode;
    this.count = count;
  }

  /**
   * Always uses {@code replicas} replicas.
   *
   * @param replicas the replica count
   * @return the policy
   * @throws IllegalArgumentException when {@code replicas} is negative
   */
  public static ReplicaPolicy fixed(int replicas) {
    return new ReplicaPolicy(Mode.FIXED, replicas);
  }

  /**
   * Uses one replica fewer than there are data nodes, between 0 and {@code max}.
   *
   * @param max the most replicas
   * @return the policy
   * @throws IllegalArgumentException when {@code max} is negative
   */
  public static ReplicaPolicy dataNodesMinusOne(int max) {
    return new ReplicaPolicy(Mode.DATA_NODES_MINUS_ONE, max);
  }

  /** Returns how the count is derived. */
  public Mode mode() {
    return mode;
  }

  /** Returns the fixed count, or the upper bound for {@link Mode#DATA_NODES_MINUS_ONE}. */
  public int count() {
    return count;
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

  @Override
  public boolean equals(Object other) {
    return other instanceof ReplicaPolicy that && mode == that.mode && count == that.count;
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode, count);
  }

  @Override
  public String toString() {
    return "ReplicaPolicy[mode=" + mode + ", count=" + count + "]";
  }
}
