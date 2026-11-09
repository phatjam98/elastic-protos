package com.phatjam98.elasticprotos.migration;

import java.util.List;

/**
 * The differences between an index's current mappings and the mappings a proto now produces.
 *
 * <p>Paths are dotted index paths. Multi-fields appear under their parent, for example
 * {@code title.keyword}. Parameter changes use {@code <path>:<parameter>}, for example
 * {@code title:analyzer}, with {@code <root>} as the path of root-level parameters.
 *
 * @param added fields present only in the desired mappings
 * @param removed fields present only in the current mappings
 * @param typeChanged fields whose type differs, including object versus nested
 * @param paramChanged mapping parameters that differ on fields present in both
 * @param settingsChanged whether a desired setting other than shard and replica counts differs
 * @param unindexedAdditions the subset of {@code added} that existing documents were never indexed
 *     into: a multi-field added under an existing field, or a property added under a parent whose
 *     current effective {@code dynamic} is {@code false} or {@code runtime} (old values sit in
 *     {@code _source} unindexed). Putting the mapping would leave them empty for every old
 *     document, so they need a reindex
 */
public record MappingDiff(
    List<Change> added,
    List<Change> removed,
    List<Change> typeChanged,
    List<Change> paramChanged,
    boolean settingsChanged,
    List<Change> unindexedAdditions) {

  /** Copies the lists. */
  public MappingDiff {
    added = List.copyOf(added);
    removed = List.copyOf(removed);
    typeChanged = List.copyOf(typeChanged);
    paramChanged = List.copyOf(paramChanged);
    unindexedAdditions = List.copyOf(unindexedAdditions);
  }

  /**
   * Creates a diff in which every addition can be backfilled by putting the mapping.
   *
   * @param added fields present only in the desired mappings
   * @param removed fields present only in the current mappings
   * @param typeChanged fields whose type differs
   * @param paramChanged mapping parameters that differ
   * @param settingsChanged whether a compared setting differs
   */
  public MappingDiff(
      List<Change> added,
      List<Change> removed,
      List<Change> typeChanged,
      List<Change> paramChanged,
      boolean settingsChanged) {
    this(added, removed, typeChanged, paramChanged, settingsChanged, List.of());
  }

  /**
   * One difference.
   *
   * @param path the field path, or {@code <path>:<parameter>} for a parameter change
   * @param before the current value, or null when absent
   * @param after the desired value, or null when absent
   */
  public record Change(String path, Object before, Object after) {}

  /** Returns a diff without differences. */
  public static MappingDiff empty() {
    return new MappingDiff(List.of(), List.of(), List.of(), List.of(), false);
  }

  /** Returns whether neither the mappings nor the compared settings differ. */
  public boolean isEmpty() {
    return !hasFieldChanges() && !settingsChanged;
  }

  /** Returns whether any field was added, removed or changed. */
  public boolean hasFieldChanges() {
    return !added.isEmpty() || !removed.isEmpty() || !typeChanged.isEmpty()
        || !paramChanged.isEmpty();
  }

  /**
   * Returns whether fields were added and nothing was removed or changed. An additive diff can
   * still need a reindex; see {@link #unindexedAdditions()}.
   */
  public boolean additiveOnly() {
    return !added.isEmpty() && removed.isEmpty() && typeChanged.isEmpty()
        && paramChanged.isEmpty();
  }
}
