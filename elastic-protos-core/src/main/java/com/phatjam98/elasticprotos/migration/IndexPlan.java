package com.phatjam98.elasticprotos.migration;

import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import java.util.List;

/**
 * What the migrator will do for one message.
 *
 * @param target the index the proto describes
 * @param action the decision
 * @param currentIndex the concrete index the alias points at now, or null when there is none
 * @param currentHash the mapping hash recorded on the current index, {@code legacy} when it has
 *     none, or null when there is no current index
 * @param diff the differences between the current and desired mappings; empty when there is no
 *     current index
 * @param reason why the action was chosen
 * @param warnings things that will not be applied or deserve attention
 * @param writeBlocked whether the current index has {@code index.blocks.write=true}; see {@link
 *     MigrationOptions#clearStaleWriteBlock()}
 */
public record IndexPlan(
    IndexDefinition target,
    Action action,
    String currentIndex,
    String currentHash,
    MappingDiff diff,
    String reason,
    List<String> warnings,
    boolean writeBlocked) {

  /** Copies the warnings. */
  public IndexPlan {
    warnings = List.copyOf(warnings);
  }

  /** Creates a plan for an item whose current index, if any, is not write-blocked. */
  public IndexPlan(
      IndexDefinition target,
      Action action,
      String currentIndex,
      String currentHash,
      MappingDiff diff,
      String reason,
      List<String> warnings) {
    this(target, action, currentIndex, currentHash, diff, reason, warnings, false);
  }

  /** Returns the alias being migrated. */
  public String alias() {
    return target.alias();
  }
}
