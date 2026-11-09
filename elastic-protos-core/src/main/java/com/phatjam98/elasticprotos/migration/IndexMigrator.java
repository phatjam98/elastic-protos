package com.phatjam98.elasticprotos.migration;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.phatjam98.elasticprotos.engine.AliasAction;
import com.phatjam98.elasticprotos.engine.Pit;
import com.phatjam98.elasticprotos.engine.ReindexRequest;
import com.phatjam98.elasticprotos.engine.ReindexResult;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.engine.UpdateByQueryRequest;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.mapping.IndexNamingStrategy;
import com.phatjam98.elasticprotos.mapping.ProtoMapper;
import com.phatjam98.elasticprotos.migration.MappingDiff.Change;
import com.phatjam98.elasticprotos.migration.MigrationListener.Phase;
import com.phatjam98.elasticprotos.migration.MigrationResult.Outcome;
import com.phatjam98.elasticprotos.migration.MigrationResult.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates and migrates the indices behind proto messages.
 *
 * <p>{@link #plan} compares what each message maps to with what the alias points at and decides
 * on an {@link Action}; {@link #apply} carries the plan out; {@link #bootstrap} does both and is
 * meant to run once at application startup, from a single instance or job.
 *
 * <p><b>Reindex.</b> By default ({@link ReindexMode#ZERO_DOWNTIME}) the old index keeps taking
 * writes through the alias while it is copied:
 *
 * <ol>
 *   <li>the new index is created (or a leftover reused) and marked {@code
 *       _meta.elastic_protos.pending = {action: reindex, from, phase: copying}};
 *   <li>a {@code _reindex} pass with external versioning copies the old index, and
 *   <li>a second, identical pass catches up with the writes made during the first;
 *   <li>a point in time over the new index is opened as its snapshot and recorded in the marker
 *       ({@code phase: swapping});
 *   <li>the alias is swapped atomically;
 *   <li>after {@link MigrationOptions#swapGrace()} the old index is write-blocked ({@code phase:
 *       reconciling});
 *   <li>every document of the frozen old index that the snapshot lacks is created in the new
 *       index, and every one whose version differs is overwritten on the snapshot's sequence
 *       number and primary term, so a write through the alias after the swap always wins (409);
 *   <li>documents of the snapshot that the old index no longer has are deleted the same way;
 *   <li>the marker is removed and the old index retained write-blocked, or deleted with {@link
 *       MigrationOptions#deleteOldAfter()}.
 * </ol>
 *
 * <p>A crash before the swap restarts from the copy on the next run, reusing the new index; a
 * crash after it resumes at the freeze, never copying with {@code _reindex} again, because
 * external versioning compares version numbers and the new index takes live writes. When the
 * snapshot's point in time is gone, the reconcile only creates missing documents and reports what
 * it could not decide as warnings. A migration script also runs on the reconciled documents,
 * through a staging index {@code <new index>-reconcile}; it must not change {@code _id}.
 *
 * <p>With {@link ReindexMode#BLOCKING} the old index is write-blocked from the moment the copy
 * starts until the alias has been swapped, so writes fail for that window, and the document counts
 * of both indices must match. {@link #rollback(IndexPlan, String, RollbackMode)} points the alias
 * back at the old index, by default copying the writes made since back into it.
 *
 * <p><b>Backfill.</b> Fields that existing documents were never indexed into ({@link
 * MappingDiff#unindexedAdditions()}) are added to the mapping in place and every document is
 * re-indexed with an update by query ({@link Action#BACKFILL}), under a {@code pending = {action:
 * backfill}} marker that makes the next plan backfill again if the run is interrupted. With
 * {@link MigrationOptions#backfill()} false, or when a migration script exists for the change, they
 * are reindexed instead.
 *
 * <p>When a reindex fails before the swap, the new index is deleted, but only if this run created
 * it and the alias does not point at it, and a blocking reindex unblocks the old index. An index
 * that already has the target name when a reindex starts is a leftover from an interrupted run: it
 * is reused when its recorded hash is the target hash and recreated otherwise. A write block found
 * on the index an alias points at is assumed to be left by an interrupted blocking reindex and is
 * cleared when that index is kept or copied without blocking, unless {@link
 * MigrationOptions#clearStaleWriteBlock()} is false.
 *
 * <p>A {@code PUT_MAPPING} keeps the index name while the recorded hash moves on, so a later
 * reindex can find that the alias already points at an index with the target name. The new index
 * then gets the lowest free generation of that name, {@code <name>-g2}, {@code <name>-g3} and so
 * on (see {@link IndexNamingStrategy#generationName}). The same happens when an index with the
 * target name exists and is write-blocked: it is an old index retained by an earlier reindex,
 * kept for rollback, and is never reused or deleted.
 *
 * <p>Running the migrator for the same aliases from several processes at once is unsupported and
 * can be destructive: one process can clear the write block or delete the index another is
 * reindexing into. Run it from one instance or a dedicated job.
 */
public final class IndexMigrator {

  private static final Logger LOG = LoggerFactory.getLogger(IndexMigrator.class);

  /** The hash reported for indices created without elastic-protos metadata. */
  public static final String LEGACY_HASH = "legacy";

  /** The highest index generation tried before a name collision is reported as blocked. */
  private static final int MAX_GENERATION = 100;

  /** The shortest keep-alive of the snapshot point in time of a zero-downtime reindex. */
  private static final Duration MIN_PIT_KEEP_ALIVE = Duration.ofHours(1);

  /** The suffix of the staging index a migration script reconciles through. */
  static final String STAGING_SUFFIX = "-reconcile";

  private static final String PENDING = "pending";
  private static final String ACTION_REINDEX = "reindex";
  private static final String ACTION_BACKFILL = "backfill";
  private static final String PHASE_COPYING = "copying";
  private static final String PHASE_SWAPPING = "swapping";
  private static final String PHASE_RECONCILING = "reconciling";
  private static final String PHASE_FINISHING = "finishing";

  private final SearchEngine engine;
  private final ProtoMapper mapper;
  private final MigrationOptions options;
  private final MigrationScriptProvider scripts;
  private final MigrationListener listener;

  private IndexMigrator(Builder builder) {
    this.engine = Objects.requireNonNull(builder.engine, "engine is required");
    this.mapper = builder.mapper;
    this.options = builder.options;
    this.scripts = builder.scripts;
    this.listener = builder.listener;
  }

  /** Starts building a migrator. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns the options in effect. */
  public MigrationOptions options() {
    return options;
  }

  // ------------------------------------------------------------------ planning

  /**
   * Plans the migration of messages, in the order given. Messages marked {@code skip} are left
   * out.
   *
   * @param messages root message descriptors
   * @return the plan
   */
  public MigrationPlan plan(List<Descriptor> messages) {
    List<IndexPlan> items = new ArrayList<>();
    for (Descriptor descriptor : messages) {
      if (mapper.skips(descriptor)) {
        continue;
      }
      items.add(planOne(mapper.map(descriptor)));
    }
    return new MigrationPlan(items);
  }

  /**
   * Plans the migration of generated message classes.
   *
   * @param types generated protobuf message classes
   * @return the plan
   */
  @SafeVarargs
  public final MigrationPlan plan(Class<? extends Message>... types) {
    List<Descriptor> descriptors = new ArrayList<>();
    for (Class<? extends Message> type : types) {
      descriptors.add(ProtoMapper.descriptorOf(type));
    }
    return plan(descriptors);
  }

  /** Adds the warning for a write-blocked current index and marks the plan. */
  private IndexPlan withWriteBlock(IndexPlan plan) {
    String warning = "index " + plan.currentIndex() + " has a write block (index.blocks.write)"
        + ", probably left by an interrupted reindex; ";
    boolean copiedLive = plan.action() == Action.REINDEX && plan.resume() == null
        && options.reindexMode() == ReindexMode.ZERO_DOWNTIME;
    switch (plan.action()) {
      case NO_OP:
      case PUT_MAPPING:
      case BACKFILL:
      case REINDEX:
        if (plan.action() == Action.REINDEX && !copiedLive) {
          warning += "the reindex moves the alias to an unblocked index";
          break;
        }
        warning += options.clearStaleWriteBlock()
            ? "it will be cleared (clearStaleWriteBlock)"
            : "it is left in place (clearStaleWriteBlock is false) and writes through "
                + plan.alias() + " fail until it is cleared";
        break;
      default:
        warning += "writes through " + plan.alias() + " fail until it is cleared";
        break;
    }
    List<String> warnings = new ArrayList<>(plan.warnings());
    warnings.add(warning);
    return new IndexPlan(plan.target(), plan.action(), plan.currentIndex(), plan.currentHash(),
        plan.diff(), plan.reason(), warnings, true, plan.resume());
  }

  /** Reads {@code index.blocks.write} from settings as engines return them (nested, strings). */
  private static boolean isWriteBlocked(Map<String, Object> settings) {
    Object write = map(map(map(settings.get("index")).get("blocks"))).get("write");
    return "true".equals(String.valueOf(write));
  }

  private IndexPlan planOne(IndexDefinition target) {
    String alias = target.alias();
    Map<String, Set<String>> aliases = engine.getAliases(alias);
    if (aliases.containsKey(alias)) {
      return blocked(target, null, null,
          "a concrete index named like the alias exists: " + alias);
    }
    List<String> holders = new ArrayList<>();
    aliases.forEach((index, names) -> {
      if (names.contains(alias)) {
        holders.add(index);
      }
    });
    if (holders.isEmpty()) {
      if (engine.indexExists(alias)) {
        return blocked(target, null, null,
            "a concrete index named like the alias exists: " + alias);
      }
      return new IndexPlan(target, Action.CREATE, null, null, MappingDiff.empty(),
          "alias " + alias + " does not exist", List.of());
    }
    if (holders.size() > 1) {
      return blocked(target, null, null,
          "alias " + alias + " points at " + holders.size() + " indices: " + holders);
    }

    String current = holders.get(0);
    Map<String, Object> mappings = engine.getMappings(current);
    Map<String, Object> pending = pendingOf(mappings);
    Object from = pending.get("from");
    if (ACTION_REINDEX.equals(pending.get("action")) && from != null
        && !current.equals(from.toString())) {
      return resumePlan(target, current, mappings, pending);
    }
    Map<String, Object> settings = engine.getSettings(current);
    IndexPlan plan = planExisting(target, current, mappings, settings);
    if (ACTION_BACKFILL.equals(pending.get("action")) && (plan.action() == Action.NO_OP
        || plan.action() == Action.PUT_MAPPING || plan.action() == Action.BACKFILL)) {
      plan = new IndexPlan(plan.target(), Action.BACKFILL, current, plan.currentHash(),
          plan.diff(), "resuming the interrupted backfill of " + current, plan.warnings());
    }
    return isWriteBlocked(settings) ? withWriteBlock(plan) : plan;
  }

  /**
   * Plans the rest of a zero-downtime reindex or rollback that moved the alias to {@code current}
   * and did not finish: the marker on {@code current} names the old index.
   */
  private IndexPlan resumePlan(IndexDefinition target, String current,
      Map<String, Object> mappings, Map<String, Object> pending) {
    String from = pending.get("from").toString();
    boolean rollback = Boolean.TRUE.equals(pending.get("rollback"));
    ReindexResume resume = new ReindexResume(from, string(pending.get("pit")),
        string(pending.get("phase")), true, rollback);
    String fromHash = engine.indexExists(from) ? recordedHash(engine.getMappings(from)) : null;
    List<String> warnings = new ArrayList<>();
    String hash = recordedHash(mappings);
    if (!rollback && !hash.equals(target.contentHash())) {
      warnings.add(current + " records hash " + hash + " but the proto now maps to "
          + target.contentHash() + "; plan again once the interrupted reindex has finished");
    }
    return new IndexPlan(renamed(target, current), Action.REINDEX, from, fromHash,
        MappingDiff.empty(), "resuming the interrupted " + (rollback ? "rollback" : "reindex")
            + " from " + from + " into " + current + ", which the alias already points at"
            + " (phase " + resume.phase() + ")", warnings, false, resume);
  }

  private IndexPlan planExisting(IndexDefinition target, String current,
      Map<String, Object> mappings, Map<String, Object> settings) {
    final String currentHash = recordedHash(mappings);
    MappingDiff diff = DiffClassifier.diff(mappings, target.mappings(), settings,
        target.settings());
    List<String> warnings = new ArrayList<>(
        DiffClassifier.settingsWarnings(settings, target.settings()));
    if (diff.settingsChanged()) {
      warnings.add("settings differ; not applied");
    }
    if (!diff.removed().isEmpty()) {
      warnings.add("fields no longer in the proto: " + paths(diff.removed())
          + (options.removedFields() == RemovedFieldPolicy.KEEP ? " (kept in the mapping)" : ""));
    }

    for (Change change : diff.paramChanged()) {
      if (change.path().endsWith(":dynamic") && "false".equals(String.valueOf(change.before()))
          && "true".equals(String.valueOf(change.after()))) {
        String path = change.path().substring(0, change.path().length() - ":dynamic".length());
        warnings.add(path + " becomes dynamic: values existing documents carry under it that"
            + " were not mapped stay unindexed until those documents are written again");
      }
    }

    boolean upToDate = target.contentHash().equals(currentHash);
    if ((upToDate || current.equals(target.name())) && !diff.hasFieldChanges()) {
      return new IndexPlan(target, Action.NO_OP, current, currentHash, diff,
          upToDate ? upToDate(currentHash) : "index " + current + " is up to date", warnings);
    }
    Action action = DiffClassifier.classify(diff, options.removedFields());
    String instead = "";
    if (action == Action.BACKFILL) {
      String fromHash = currentHash == null ? LEGACY_HASH : currentHash;
      if (!options.backfill()) {
        action = Action.REINDEX;
        instead = " (backfill is disabled)";
      } else if (scripts.script(target.alias(), fromHash, target.contentHash()).isPresent()) {
        action = Action.REINDEX;
        instead = " (a migration script exists for " + fromHash + " -> "
            + target.contentHash() + ", so the documents are copied through it)";
      }
    }
    switch (action) {
      case NO_OP:
        return new IndexPlan(target, action, current, currentHash, diff,
            upToDate ? upToDate(currentHash) : "mappings of " + current + " already match",
            warnings);
      case PUT_MAPPING:
        return new IndexPlan(target, action, current, currentHash, diff,
            "compatible change: added " + paths(diff.added()), warnings);
      case BACKFILL:
        return new IndexPlan(target, action, current, currentHash, diff,
            "added " + paths(diff.unindexedAdditions()) + ", which existing documents were never"
                + " indexed into: the mapping is extended in place and the documents re-indexed",
            warnings);
      default:
        String breaking = describeBreaking(diff) + instead;
        if (!options.allowReindex()) {
          return new IndexPlan(target, Action.BLOCKED, current, currentHash, diff,
              "breaking change needs a reindex but allowReindex is false: " + breaking,
              warnings);
        }
        List<String> skipped = new ArrayList<>();
        String next = reindexTarget(target, current, skipped);
        if (next == null) {
          return new IndexPlan(target, Action.BLOCKED, current, currentHash, diff,
              String.join("; ", skipped) + ", and no free generation of " + target.name()
                  + " was found: " + breaking, warnings);
        }
        String reason = "breaking change: " + breaking;
        if (!skipped.isEmpty()) {
          reason += "; " + String.join("; ", skipped) + ", so the new index is " + next;
        }
        ReindexResume resume = interruptedCopy(current, next);
        if (resume != null) {
          reason += "; an interrupted zero-downtime reindex into " + next + " (phase "
              + resume.phase() + ") restarts from the copy";
        }
        return new IndexPlan(next.equals(target.name()) ? target : renamed(target, next),
            Action.REINDEX, current, currentHash, diff, reason, warnings, false, resume);
    }
  }

  /**
   * Finds the marker of a zero-downtime reindex from {@code current} into {@code next} that
   * stopped before the alias swap. Only looked for in {@link ReindexMode#ZERO_DOWNTIME}.
   */
  private ReindexResume interruptedCopy(String current, String next) {
    if (options.reindexMode() != ReindexMode.ZERO_DOWNTIME || !engine.indexExists(next)) {
      return null;
    }
    Map<String, Object> pending = pendingOf(engine.getMappings(next));
    if (!ACTION_REINDEX.equals(pending.get("action"))
        || !current.equals(string(pending.get("from")))) {
      return null;
    }
    return new ReindexResume(current, string(pending.get("pit")),
        string(pending.get("phase")), false, Boolean.TRUE.equals(pending.get("rollback")));
  }

  /**
   * Picks the index to reindex into: the target name itself (generation 1) or the lowest later
   * generation ({@link IndexNamingStrategy#generationName}) that fits. A candidate is skipped when
   * it is the index the alias points at (a {@code PUT_MAPPING} keeps the old name while the hash
   * moves on, so reverting names it again) or when it exists and is write-blocked, since that is
   * an old index retained after an earlier reindex: a rollback copy that is never reused or
   * deleted. Otherwise generation 1 is taken whether or not it exists ({@link #prepareTarget}
   * reuses or recreates a leftover), and a later generation is taken when it does not exist or
   * is a leftover recording the target hash.
   *
   * @param skipped receives one note per skipped candidate, for the plan reason
   * @return the name, or null when none of the first {@value #MAX_GENERATION} generations fits
   */
  private String reindexTarget(IndexDefinition target, String current, List<String> skipped) {
    IndexNamingStrategy naming = mapper.naming();
    for (int generation = 1; generation <= MAX_GENERATION; generation++) {
      String name = generation == 1
          ? target.name() : naming.generationName(target.name(), generation);
      if (name.equals(current)) {
        if (generation == 1) {
          skipped.add(current + " already has the target name");
        }
        continue;
      }
      if (!engine.indexExists(name)) {
        return name;
      }
      if (isWriteBlocked(engine.getSettings(name))) {
        skipped.add(name + " exists and is write-blocked (retained from an earlier reindex)");
        continue;
      }
      if (generation == 1
          || recordedHash(engine.getMappings(name)).equals(target.contentHash())) {
        return name;
      }
    }
    return null;
  }

  private static IndexDefinition renamed(IndexDefinition target, String name) {
    return new IndexDefinition(target.descriptor(), target.alias(), name, target.contentHash(),
        target.settings(), target.mappings(), target.fields(), target.nestedPaths(),
        target.fullTextFields(), target.ignoredPaths(), target.idField(), target.warnings());
  }

  private static String upToDate(String hash) {
    return "up to date (hash " + hash + ")";
  }

  private static IndexPlan blocked(
      IndexDefinition target, String current, String hash, String reason) {
    return new IndexPlan(target, Action.BLOCKED, current, hash, MappingDiff.empty(), reason,
        List.of());
  }

  private static String recordedHash(Map<String, Object> mappings) {
    Object hash = map(map(mappings.get("_meta")).get(ProtoMapper.META_KEY)).get("hash");
    return hash == null ? LEGACY_HASH : hash.toString();
  }

  /** Returns {@code _meta.elastic_protos.pending} of mappings, or an empty map. */
  private static Map<String, Object> pendingOf(Map<String, Object> mappings) {
    return map(map(map(mappings.get("_meta")).get(ProtoMapper.META_KEY)).get(PENDING));
  }

  /** Returns a copy of the {@code _meta} object of mappings. */
  private static Map<String, Object> metaOf(Map<String, Object> mappings) {
    return new TreeMap<>(map(mappings.get("_meta")));
  }

  private static String string(Object value) {
    return value == null ? null : value.toString();
  }

  private static String describeBreaking(MappingDiff diff) {
    List<String> parts = new ArrayList<>();
    for (Change change : diff.typeChanged()) {
      parts.add(change.path() + " type " + change.before() + " -> " + change.after());
    }
    for (Change change : diff.paramChanged()) {
      parts.add(change.path() + " " + change.before() + " -> " + change.after());
    }
    if (!diff.unindexedAdditions().isEmpty()) {
      parts.add("added " + paths(diff.unindexedAdditions())
          + ", which existing documents were never indexed into");
    }
    if (!diff.removed().isEmpty()) {
      parts.add("removed " + paths(diff.removed()));
    }
    return String.join(", ", parts);
  }

  private static List<String> paths(List<Change> changes) {
    return changes.stream().map(Change::path).toList();
  }

  // ------------------------------------------------------------------ applying

  /**
   * Carries out a plan. Nothing is changed when the options ask for a dry run.
   *
   * @param plan the plan from {@link #plan}
   * @return one outcome per attempted item
   */
  public MigrationResult apply(MigrationPlan plan) {
    List<Outcome> outcomes = new ArrayList<>();
    for (IndexPlan item : plan.items()) {
      if (options.dryRun()) {
        LOG.info("dry run: {} {} ({})", item.action(), item.alias(), item.reason());
        outcomes.add(new Outcome(item, Status.DRY_RUN, expectedIndex(item), List.of(), null));
        continue;
      }
      Outcome outcome = applyOne(item);
      outcomes.add(outcome);
      if (outcome.status() == Status.FAILED && options.failure() == FailurePolicy.FAIL_FAST) {
        LOG.info("stopping after the failure of {} (FAIL_FAST)", item.alias());
        break;
      }
    }
    return new MigrationResult(outcomes);
  }

  /**
   * Plans and applies the migration of generated message classes.
   *
   * @param types generated protobuf message classes
   * @return the outcomes
   */
  @SafeVarargs
  public final MigrationResult bootstrap(Class<? extends Message>... types) {
    return apply(plan(types));
  }

  /**
   * Points an alias back at the index it used before a reindex, by default copying the writes made
   * to the new index since then back into it ({@link RollbackMode#REVERSE_DELTA}).
   *
   * <p>The alias must point at the plan's target index only. {@link RollbackMode#REVERSE_DELTA}
   * unblocks the previous index, copies the new index into it with the zero-downtime procedure
   * (without a migration script), swaps the alias and retains the new index write-blocked. Any
   * failure before the swap aborts the rollback: the alias stays on the new index, the previous
   * index is write-blocked again and the outcome is {@link MigrationResult.Status#FAILED}; {@link
   * RollbackMode#ALIAS_ONLY} then points the alias back without copying. {@link
   * RollbackMode#ALIAS_ONLY} unblocks the previous index, swaps the alias, waits {@link
   * MigrationOptions#swapGrace()} and write-blocks the new index.
   *
   * @param plan the reindex item that was applied
   * @param previousIndex the index to restore, usually {@link IndexPlan#currentIndex()}
   * @param mode how to restore it
   * @return the outcome: {@code index} is the previous index and {@code retainedIndices} the new
   *     one when it was applied
   */
  public Outcome rollback(IndexPlan plan, String previousIndex, RollbackMode mode) {
    Objects.requireNonNull(previousIndex, "previousIndex");
    Objects.requireNonNull(mode, "mode");
    String alias = plan.alias();
    String next = plan.target().name();
    LOG.info("rolling back {} ({}): alias from {} to {}", alias, mode, next, previousIndex);
    try {
      Set<String> holders = aliasHolders(alias);
      if (holders == null || !holders.equals(Set.of(next))) {
        throw new MigrationException("alias " + alias + " "
            + (holders == null ? "could not be read" : "points at " + holders)
            + ", not at " + next + " only; nothing was rolled back");
      }
      if (mode == RollbackMode.ALIAS_ONLY) {
        engine.setWriteBlock(previousIndex, false);
        engine.updateAliases(List.of(AliasAction.remove(next, alias),
            AliasAction.add(previousIndex, alias)));
        pause(options.swapGrace());
        engine.setWriteBlock(next, true);
        return new Outcome(plan, Status.APPLIED, previousIndex, List.of(next), null);
      }
      return reverseDelta(plan, previousIndex, next);
    } catch (Interrupted e) {
      return interrupted(plan, e);
    } catch (RuntimeException e) {
      LOG.warn("{}: rollback failed: {}", alias, e.getMessage());
      return new Outcome(plan, Status.FAILED, next, List.of(), e);
    }
  }

  /**
   * Rolls back a reindex to {@code previousIndex} with {@link RollbackMode#REVERSE_DELTA}.
   *
   * @param plan the reindex item that was applied
   * @param previousIndex the index to restore, usually {@link IndexPlan#currentIndex()}
   * @return the outcome
   */
  public Outcome rollback(IndexPlan plan, String previousIndex) {
    return rollback(plan, previousIndex, RollbackMode.REVERSE_DELTA);
  }

  /**
   * Rolls back a reindex to the index the plan found, {@link IndexPlan#currentIndex()}, with
   * {@link RollbackMode#REVERSE_DELTA}.
   *
   * @param plan the reindex item that was applied
   * @return the outcome
   * @throws MigrationException when the plan has no previous index
   */
  public Outcome rollback(IndexPlan plan) {
    if (plan.currentIndex() == null) {
      throw new MigrationException("plan for " + plan.alias() + " has no previous index");
    }
    return rollback(plan, plan.currentIndex());
  }

  private Outcome reverseDelta(IndexPlan plan, String previous, String next) {
    String alias = plan.alias();
    Map<String, Object> meta = metaOf(engine.getMappings(previous));
    engine.setWriteBlock(previous, false);
    Pit pit;
    try {
      pit = copyAndSnapshot(alias, next, previous, meta, true, null);
    } catch (Interrupted e) {
      throw e;
    } catch (RuntimeException e) {
      abortRollback(alias, previous, meta, null);
      return new Outcome(plan, Status.FAILED, next, List.of(), rollbackAborted(alias, next, e));
    }
    try {
      engine.updateAliases(List.of(AliasAction.remove(next, alias),
          AliasAction.add(previous, alias)));
    } catch (RuntimeException e) {
      if (!Set.of(previous).equals(aliasHolders(alias))) {
        abortRollback(alias, previous, meta, pit);
        return new Outcome(plan, Status.FAILED, next, List.of(), rollbackAborted(alias, next, e));
      }
      LOG.warn("{}: the alias swap reported a failure ({}) but the alias now points at {} only",
          alias, e.getMessage(), previous);
    }
    phase(alias, Phase.SWAPPED);
    return afterSwap(plan, new Move(alias, next, previous, meta, pit.id(), null, true, null,
        false));
  }

  private static MigrationException rollbackAborted(String alias, String next, Exception e) {
    return new MigrationException("rollback of " + alias + " aborted before the alias swap: "
        + e.getMessage() + "; the alias still points at " + next
        + ". RollbackMode.ALIAS_ONLY points it back without copying", e);
  }

  /** Undoes the preparation of a reverse-delta rollback that stopped before the swap. */
  private void abortRollback(String alias, String previous, Map<String, Object> meta, Pit pit) {
    closeQuietly(alias, pit);
    try {
      writePending(previous, meta, null);
    } catch (RuntimeException e) {
      LOG.warn("{}: could not remove the marker from {}: {}", alias, previous, e.getMessage());
    }
    try {
      engine.setWriteBlock(previous, true);
    } catch (RuntimeException e) {
      LOG.warn("{}: could not write-block {} again: {}", alias, previous, e.getMessage());
    }
  }

  private static String expectedIndex(IndexPlan item) {
    switch (item.action()) {
      case CREATE:
      case REINDEX:
        return item.target().name();
      default:
        return item.currentIndex();
    }
  }

  private Outcome applyOne(IndexPlan item) {
    try {
      switch (item.action()) {
        case CREATE:
          return create(item);
        case PUT_MAPPING:
          LOG.info("{}: adding fields {} to {}", item.alias(), paths(item.diff().added()),
              item.currentIndex());
          engine.putMappings(item.currentIndex(), item.target().mappings());
          clearStaleWriteBlock(item);
          return new Outcome(item, Status.APPLIED, item.currentIndex(), List.of(), null);
        case BACKFILL:
          return backfill(item);
        case REINDEX:
          return reindex(item);
        case BLOCKED:
          LOG.info("{}: blocked: {}", item.alias(), item.reason());
          return new Outcome(item, Status.BLOCKED, item.currentIndex(), List.of(), null);
        default:
          LOG.info("{}: {} is up to date", item.alias(), item.currentIndex());
          return new Outcome(item, clearStaleWriteBlock(item) ? Status.APPLIED : Status.SKIPPED,
              item.currentIndex(), List.of(), null);
      }
    } catch (Interrupted e) {
      return interrupted(item, e);
    } catch (RuntimeException e) {
      LOG.info("{}: {} failed: {}", item.alias(), item.action(), e.getMessage());
      return new Outcome(item, Status.FAILED, item.currentIndex(), List.of(), e);
    }
  }

  /** Reports a run the listener stopped, naming the index the alias points at now. */
  private Outcome interrupted(IndexPlan item, Interrupted e) {
    LOG.warn("{}: stopped by the listener after {}: {}", item.alias(), e.phase,
        e.getCause().getMessage());
    Set<String> holders = aliasHolders(item.alias());
    String index = holders != null && holders.size() == 1
        ? holders.iterator().next() : item.currentIndex();
    return new Outcome(item, Status.FAILED, index, List.of(), new MigrationException(
        item.alias() + ": stopped by the listener after " + e.phase + ": "
            + e.getCause().getMessage(), e.getCause()));
  }

  private boolean clearStaleWriteBlock(IndexPlan item) {
    if (!item.writeBlocked() || !options.clearStaleWriteBlock()) {
      return false;
    }
    LOG.warn("{}: clearing the write block on {}, assumed to be left by an interrupted reindex",
        item.alias(), item.currentIndex());
    engine.setWriteBlock(item.currentIndex(), false);
    return true;
  }

  private Outcome create(IndexPlan item) {
    IndexDefinition target = item.target();
    LOG.info("{}: creating {} with the alias", target.alias(), target.name());
    engine.createIndex(target.name(), creationSettings(target), target.mappings(),
        Set.of(target.alias()));
    return new Outcome(item, Status.APPLIED, target.name(), List.of(), null);
  }

  private Map<String, Object> creationSettings(IndexDefinition target) {
    Map<String, Object> settings = new TreeMap<>(target.settings());
    settings.putIfAbsent("number_of_shards", options.defaultShards());
    if (!settings.containsKey("number_of_replicas")) {
      ReplicaPolicy policy = options.replicas();
      int dataNodes = policy.needsDataNodes() ? engine.health().numberOfDataNodes() : 0;
      settings.put("number_of_replicas", policy.replicas(dataNodes));
    }
    return settings;
  }

  private Outcome reindex(IndexPlan item) {
    if (item.resume() != null && item.resume().swapped()) {
      return resumeAfterSwap(item);
    }
    if (options.reindexMode() == ReindexMode.BLOCKING) {
      return reindexBlocking(item);
    }
    return reindexZeroDowntime(item);
  }

  private Outcome reindexBlocking(IndexPlan item) {
    IndexDefinition target = item.target();
    String alias = target.alias();
    String current = item.currentIndex();
    String next = target.name();

    final boolean createdNext = prepareTarget(target, current);
    if (!createdNext) {
      Map<String, Object> leftover = engine.getMappings(next);
      if (!pendingOf(leftover).isEmpty()) {
        LOG.info("{}: removing the marker of an interrupted zero-downtime reindex from {}",
            alias, next);
        writePending(next, metaOf(leftover), null);
      }
    }
    LOG.info("{}: blocking writes to {}", alias, current);
    engine.setWriteBlock(current, true);
    long before;
    long after;
    List<String> warnings = new ArrayList<>();
    try {
      engine.refresh(current);
      ReindexRequest request = ReindexRequest.of(current, next)
          .withTimeout(options.reindexTimeout());
      String fromHash = item.currentHash() == null ? LEGACY_HASH : item.currentHash();
      Optional<String> script = scripts.script(alias, fromHash, target.contentHash());
      if (script.isPresent()) {
        LOG.info("{}: applying migration script {} -> {}", alias, fromHash,
            target.contentHash());
        request = request.withScript(script.get(),
            Map.of("alias", alias, "from_hash", fromHash, "to_hash", target.contentHash()));
      }
      LOG.info("{}: reindexing {} -> {}", alias, current, next);
      ReindexResult result = engine.reindex(request);
      if (!result.ok()) {
        throw new MigrationException("reindex " + current + " -> " + next + " failed: "
            + (result.timedOut() ? "timed out" : result.failures().size() + " failures, first "
                + result.failures().get(0)));
      }
      before = engine.count(current);
      after = engine.count(next);
      if (before != after) {
        if (script.isEmpty()) {
          throw new MigrationException("reindex " + current + " -> " + next + " copied " + after
              + " of " + before + " documents");
        }
        // A script may drop or add documents on purpose; the engine reported no failures.
        String warning = "the migration script left " + after + " documents in " + next
            + " from " + before + " in " + current;
        LOG.warn("{}: {}", alias, warning);
        warnings.add(warning);
      }
      phase(alias, Phase.COPIED);
    } catch (Interrupted e) {
      throw e;
    } catch (RuntimeException e) {
      LOG.warn("{}: reindex {} -> {} failed: {}", alias, current, next, e.getMessage());
      undo(alias, current, next, createdNext);
      if (e instanceof MigrationException) {
        throw e;
      }
      throw new MigrationException("reindex " + current + " -> " + next + " failed: "
          + e.getMessage(), e);
    }

    LOG.info("{}: swapping the alias from {} to {}", alias, current, next);
    try {
      engine.updateAliases(List.of(AliasAction.remove(current, alias),
          AliasAction.add(next, alias)));
    } catch (RuntimeException e) {
      Set<String> holders = aliasHolders(alias);
      if (Set.of(next).equals(holders)) {
        LOG.warn("{}: the alias swap reported a failure ({}) but {} now points at {} only;"
            + " treating the migration as succeeded", alias, e.getMessage(), alias, next);
      } else {
        undo(alias, current, next, createdNext);
        throw new MigrationException("alias swap " + current + " -> " + next + " failed: "
            + e.getMessage(), e);
      }
    }
    phase(alias, Phase.SWAPPED);
    if (options.deleteOldAfter()) {
      LOG.info("{}: deleting old index {}", alias, current);
      engine.deleteIndex(current);
      return new Outcome(item, Status.APPLIED, next, List.of(), null, before, after, warnings);
    }
    LOG.info("{}: retaining old index {} (write-blocked)", alias, current);
    return new Outcome(item, Status.APPLIED, next, List.of(current), null, before, after,
        warnings);
  }

  // ------------------------------------------------------------------ zero-downtime reindex

  private Outcome reindexZeroDowntime(IndexPlan item) {
    IndexDefinition target = item.target();
    String alias = target.alias();
    String current = item.currentIndex();
    String next = target.name();
    if (item.writeBlocked() && options.clearStaleWriteBlock()) {
      LOG.warn("{}: clearing the write block on {}, assumed to be left by an interrupted reindex",
          alias, current);
      engine.setWriteBlock(current, false);
    }
    String fromHash = item.currentHash() == null ? LEGACY_HASH : item.currentHash();
    Reconciler.Staging staging = staging(target, next, fromHash, target.contentHash());
    Map<String, Object> meta = metaOf(target.mappings());

    final boolean createdNext = prepareTarget(target, current);
    Pit pit;
    try {
      pit = copyAndSnapshot(alias, current, next, meta, false, staging);
    } catch (Interrupted e) {
      throw e;
    } catch (RuntimeException e) {
      LOG.warn("{}: reindex {} -> {} failed: {}", alias, current, next, e.getMessage());
      undo(alias, current, next, createdNext, false);
      throw e instanceof MigrationException ? (MigrationException) e
          : new MigrationException("reindex " + current + " -> " + next + " failed: "
              + e.getMessage(), e);
    }

    LOG.info("{}: swapping the alias from {} to {}", alias, current, next);
    try {
      engine.updateAliases(List.of(AliasAction.remove(current, alias),
          AliasAction.add(next, alias)));
    } catch (RuntimeException e) {
      Set<String> holders = aliasHolders(alias);
      if (Set.of(next).equals(holders)) {
        LOG.warn("{}: the alias swap reported a failure ({}) but {} now points at {} only;"
            + " treating the swap as done", alias, e.getMessage(), alias, next);
      } else {
        closeQuietly(alias, pit);
        undo(alias, current, next, createdNext, false);
        throw new MigrationException("alias swap " + current + " -> " + next + " failed: "
            + e.getMessage(), e);
      }
    }
    phase(alias, Phase.SWAPPED);
    return afterSwap(item, new Move(alias, current, next, meta, pit.id(), staging, false, null,
        options.deleteOldAfter()));
  }

  /**
   * Marks {@code to}, copies {@code from} into it twice with external versioning (copy, then
   * catch-up) and opens the point in time over {@code to} that serves as the snapshot.
   */
  private Pit copyAndSnapshot(String alias, String from, String to, Map<String, Object> meta,
      boolean rollback, Reconciler.Staging staging) {
    writePending(to, meta, pending(from, PHASE_COPYING, null, rollback));
    phase(alias, Phase.PREPARED);
    ReindexRequest copy = ReindexRequest.of(from, to)
        .withVersionType(ReindexRequest.VersionType.EXTERNAL)
        .withProceedOnConflicts(true)
        .withRefresh(true)
        .withTimeout(options.reindexTimeout());
    if (staging != null) {
      LOG.info("{}: applying the migration script", alias);
      copy = copy.withScript(staging.script(), staging.params());
    }
    LOG.info("{}: copying {} -> {} while it takes writes", alias, from, to);
    engine.refresh(from);
    requireComplete("copy", from, to, engine.reindex(copy));
    phase(alias, Phase.COPIED);
    LOG.info("{}: catching up {} -> {}", alias, from, to);
    engine.refresh(from);
    requireComplete("catch-up", from, to, engine.reindex(copy));
    phase(alias, Phase.CAUGHT_UP);
    engine.refresh(to);
    Pit pit = engine.openPit(List.of(to), pitKeepAlive());
    try {
      writePending(to, meta, pending(from, PHASE_SWAPPING, pit.id(), rollback));
    } catch (RuntimeException e) {
      closeQuietly(alias, pit);
      throw e;
    }
    phase(alias, Phase.SNAPSHOT);
    return pit;
  }

  private void requireComplete(String pass, String from, String to, ReindexResult result) {
    if (!result.complete()) {
      throw new MigrationException(pass + " " + from + " -> " + to + " did not complete: "
          + describe(result));
    }
  }

  /** Describes why a reindex or update by query result is not complete. */
  static String describe(ReindexResult result) {
    if (result.timedOut()) {
      return "timed out";
    }
    if (!result.failures().isEmpty()) {
      return result.failures().size() + " failures, first " + result.failures().get(0);
    }
    return "only " + (result.created() + result.updated() + result.deleted() + result.noops()
        + result.versionConflicts()) + " of " + result.total() + " documents accounted for";
  }

  /** The state of a reindex or rollback after its alias swap. */
  private record Move(String alias, String from, String to, Map<String, Object> meta,
      String pitId, Reconciler.Staging staging, boolean rollback, String phase,
      boolean deleteOld) {}

  /** Continues a zero-downtime reindex or rollback whose alias swap is done. */
  private Outcome resumeAfterSwap(IndexPlan item) {
    ReindexResume resume = item.resume();
    String alias = item.alias();
    String to = item.target().name();
    String from = resume.from();
    LOG.info("{}: resuming the interrupted {} from {} into {} at phase {}", alias,
        resume.rollback() ? "rollback" : "reindex", from, to, resume.phase());
    Map<String, Object> mappings = engine.getMappings(to);
    Reconciler.Staging staging = null;
    if (!resume.rollback() && engine.indexExists(from)) {
      staging = staging(item.target(), to, recordedHash(engine.getMappings(from)),
          recordedHash(mappings));
    }
    return afterSwap(item, new Move(alias, from, to, metaOf(mappings), resume.pitId(), staging,
        resume.rollback(), resume.phase(), !resume.rollback() && options.deleteOldAfter()));
  }

  /**
   * Freezes the old index, reconciles, sweeps and removes the marker. A failure leaves the marker
   * in place, so the next plan resumes here.
   */
  private Outcome afterSwap(IndexPlan item, Move move) {
    String alias = move.alias();
    String from = move.from();
    String to = move.to();
    Reconciler.Counts counts = new Reconciler.Counts();
    long before;
    long after;
    try {
      if (!engine.indexExists(from)) {
        String warning = from + " no longer exists, so nothing was reconciled into " + to;
        LOG.warn("{}: {}", alias, warning);
        writePending(to, move.meta(), null);
        phase(alias, Phase.FINISHED);
        return new Outcome(item, Status.APPLIED, to, List.of(), null, null, null,
            List.of(warning), counts.stats());
      }
      String pitId = move.pitId();
      if (!PHASE_FINISHING.equals(move.phase())) {
        freeze(move);
        reconcile(move, counts);
        writePending(to, move.meta(), pending(from, PHASE_FINISHING, pitId, move.rollback()));
      }
      if (pitId != null) {
        closeQuietly(alias, new Pit(pitId, pitKeepAlive()));
      }
      if (move.staging() != null) {
        deleteQuietly(alias, move.staging().index());
      }
      writePending(to, move.meta(), null);
      phase(alias, Phase.FINISHED);
      engine.refresh(to);
      before = engine.count(from);
      after = engine.count(to);
    } catch (Interrupted e) {
      throw e;
    } catch (RuntimeException e) {
      LOG.warn("{}: reconciling {} -> {} after the alias swap failed: {}", alias, from, to,
          e.getMessage());
      return new Outcome(item, Status.FAILED, to, List.of(from), new MigrationException(
          "reconciling " + from + " -> " + to + " after the alias swap failed: " + e.getMessage()
              + "; the alias points at " + to + " and " + to + " keeps its pending marker, so"
              + " the next plan and apply resume the reconcile", e),
          null, null, counts.warnings, counts.stats());
    }
    List<String> warnings = new ArrayList<>(counts.warnings);
    if (before != after) {
      String warning = from + " holds " + before + " documents and " + to + " " + after
          + "; writes through the alias after the swap"
          + (move.staging() == null ? "" : " and the migration script") + " account for this";
      LOG.info("{}: {}", alias, warning);
      warnings.add(warning);
    }
    if (move.deleteOld()) {
      LOG.info("{}: deleting old index {}", alias, from);
      engine.deleteIndex(from);
      return new Outcome(item, Status.APPLIED, to, List.of(), null, before, after, warnings,
          counts.stats());
    }
    LOG.info("{}: retaining old index {} (write-blocked)", alias, from);
    return new Outcome(item, Status.APPLIED, to, List.of(from), null, before, after, warnings,
        counts.stats());
  }

  /** Waits out the swap grace, write-blocks and refreshes the old index. */
  private void freeze(Move move) {
    String from = move.from();
    if (!isWriteBlocked(engine.getSettings(from))) {
      pause(options.swapGrace());
      LOG.info("{}: blocking writes to {}", move.alias(), from);
      engine.setWriteBlock(from, true);
    }
    engine.refresh(from);
    writePending(move.to(), move.meta(),
        pending(from, PHASE_RECONCILING, move.pitId(), move.rollback()));
    phase(move.alias(), Phase.FROZEN);
  }

  /** Reconciles and sweeps against the snapshot, or reconciles without it when it is gone. */
  private void reconcile(Move move, Reconciler.Counts counts) {
    String alias = move.alias();
    Reconciler reconciler = new Reconciler(engine, pitKeepAlive(), options.reindexTimeout());
    if (move.staging() != null) {
      String staging = move.staging().index();
      if (engine.indexExists(staging)) {
        engine.deleteIndex(staging);
      }
      Map<String, Object> mappings = new TreeMap<>(engine.getMappings(move.to()));
      mappings.remove("_meta");
      engine.createIndex(staging, move.staging().settings(), mappings, Set.of());
    }
    boolean lost = move.pitId() == null;
    if (!lost) {
      try {
        LOG.info("{}: reconciling {} into {}", alias, move.from(), move.to());
        reconciler.reconcile(move.from(), move.to(), move.pitId(), move.staging(),
            move.rollback(), counts);
        phase(alias, Phase.RECONCILED);
        reconciler.sweep(move.from(), move.to(), move.pitId(), counts);
        phase(alias, Phase.SWEPT);
      } catch (Reconciler.SnapshotLost e) {
        LOG.warn("{}: {}; reconciling without it", alias, e.getMessage());
        lost = true;
      }
    }
    if (lost) {
      reconciler.degraded(move.from(), move.to(), move.staging(), move.rollback(), counts);
      phase(alias, Phase.RECONCILED);
    }
    LOG.info("{}: reconciled {} -> {}: {}", alias, move.from(), move.to(), counts.stats());
  }

  /**
   * Returns the staging for the migration script of a change, or null when there is none. The
   * staging index gets the proto's settings with one shard and no replicas.
   */
  private Reconciler.Staging staging(IndexDefinition target, String next, String fromHash,
      String toHash) {
    String alias = target.alias();
    Optional<String> script = scripts.script(alias, fromHash, toHash);
    if (script.isEmpty()) {
      return null;
    }
    Map<String, Object> settings = new TreeMap<>(target.settings());
    settings.put("number_of_shards", 1);
    settings.put("number_of_replicas", 0);
    return new Reconciler.Staging(next + STAGING_SUFFIX, script.get(),
        Map.of("alias", alias, "from_hash", fromHash, "to_hash", toHash), settings);
  }

  private Duration pitKeepAlive() {
    return options.reindexTimeout().compareTo(MIN_PIT_KEEP_ALIVE) > 0
        ? options.reindexTimeout() : MIN_PIT_KEEP_ALIVE;
  }

  private static Map<String, Object> pending(String from, String phase, String pitId,
      boolean rollback) {
    Map<String, Object> pending = new LinkedHashMap<>();
    pending.put("action", ACTION_REINDEX);
    pending.put("from", from);
    pending.put("phase", phase);
    if (pitId != null) {
      pending.put("pit", pitId);
    }
    if (rollback) {
      pending.put("rollback", true);
    }
    return pending;
  }

  /**
   * Replaces the {@code _meta} of an index with {@code meta}, its {@code elastic_protos} object
   * carrying {@code pending} or, when null, no marker. Both engines replace {@code _meta} as a
   * whole on a mapping update, so leaving the key out removes it.
   */
  private void writePending(String index, Map<String, Object> meta, Map<String, Object> pending) {
    engine.putMappings(index, Map.of("_meta", withPending(meta, pending)));
  }

  private static Map<String, Object> withPending(Map<String, Object> meta,
      Map<String, Object> pending) {
    Map<String, Object> ours = new TreeMap<>(map(meta.get(ProtoMapper.META_KEY)));
    ours.remove(PENDING);
    if (pending != null) {
      ours.put(PENDING, pending);
    }
    Map<String, Object> copy = new TreeMap<>(meta);
    copy.put(ProtoMapper.META_KEY, ours);
    return copy;
  }

  private void closeQuietly(String alias, Pit pit) {
    if (pit == null) {
      return;
    }
    try {
      engine.closePit(pit);
    } catch (RuntimeException e) {
      LOG.debug("{}: could not close the point in time: {}", alias, e.getMessage());
    }
  }

  private void deleteQuietly(String alias, String index) {
    try {
      if (engine.indexExists(index)) {
        engine.deleteIndex(index);
      }
    } catch (RuntimeException e) {
      LOG.warn("{}: could not delete {}: {}", alias, index, e.getMessage());
    }
  }

  private static void pause(Duration duration) {
    if (duration.isZero()) {
      return;
    }
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MigrationException("interrupted while waiting " + duration + " after the swap", e);
    }
  }

  /** Tells the listener a phase completed; its exception stops the run without cleanup. */
  private void phase(String alias, Phase phase) {
    try {
      listener.onPhase(alias, phase);
    } catch (RuntimeException e) {
      throw new Interrupted(phase, e);
    }
  }

  /** The listener stopped the run; nothing is cleaned up, as after a crash. */
  private static final class Interrupted extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final transient Phase phase;

    Interrupted(Phase phase, RuntimeException cause) {
      super(cause);
      this.phase = phase;
    }
  }

  // ------------------------------------------------------------------ backfill

  private Outcome backfill(IndexPlan item) {
    IndexDefinition target = item.target();
    final String alias = target.alias();
    final String current = item.currentIndex();
    clearStaleWriteBlock(item);
    Map<String, Object> meta = metaOf(target.mappings());
    Map<String, Object> pending = new LinkedHashMap<>();
    pending.put("action", ACTION_BACKFILL);
    pending.put("to_hash", target.contentHash());
    Map<String, Object> mappings = new TreeMap<>(target.mappings());
    mappings.put("_meta", withPending(meta, pending));
    LOG.info("{}: adding {} to {} and backfilling its documents", alias,
        paths(item.diff().unindexedAdditions()), current);
    engine.putMappings(current, mappings);
    phase(alias, Phase.BACKFILL_MARKED);
    ReindexResult result = engine.updateByQuery(UpdateByQueryRequest.builder()
        .index(current)
        .proceedOnConflicts(true)
        .refresh(true)
        .timeout(options.reindexTimeout())
        .build());
    if (!result.complete()) {
      throw new MigrationException("backfill of " + current + " did not complete: "
          + describe(result) + "; " + current + " keeps its backfill marker, so the next plan"
          + " backfills it again");
    }
    phase(alias, Phase.BACKFILLED);
    writePending(current, meta, null);
    phase(alias, Phase.BACKFILL_FINISHED);
    return new Outcome(item, Status.APPLIED, current, List.of(), null);
  }

  /**
   * Makes sure the reindex target exists with the target mappings. An index that already has the
   * target name is a leftover from an earlier run that did not finish: it is reused when it records
   * the target hash, and deleted and recreated otherwise, since its mappings are unknown. The index
   * the alias points at, and a write-blocked index (an old index retained after an earlier
   * reindex, which planning skips), are never touched: the item fails instead.
   *
   * @return whether this run created the index, and so may delete it if the migration fails
   */
  private boolean prepareTarget(IndexDefinition target, String current) {
    String alias = target.alias();
    String next = target.name();
    if (engine.indexExists(next)) {
      Set<String> holders = aliasHolders(alias);
      if (holders == null || holders.contains(next)) {
        throw new MigrationException("alias " + alias + " "
            + (holders == null ? "could not be read" : "already points at " + next)
            + " while migrating from " + current + "; another migration may be running."
            + " Plan again once it has finished");
      }
      if (isWriteBlocked(engine.getSettings(next))) {
        throw new MigrationException(next + " exists and is write-blocked (retained from an"
            + " earlier reindex) while migrating from " + current + "; it is neither reused nor"
            + " deleted. Plan again to pick the next generation");
      }
      String hash = recordedHash(engine.getMappings(next));
      if (hash.equals(target.contentHash())) {
        LOG.info("{}: reusing {}, left over from an earlier attempt with the target hash {}",
            alias, next, hash);
        return false;
      }
      LOG.warn("{}: {} is left over from an earlier attempt with hash {} instead of {};"
          + " deleting and recreating it", alias, next, hash, target.contentHash());
      engine.deleteIndex(next);
    }
    LOG.info("{}: creating {} for the reindex from {}", alias, next, current);
    engine.createIndex(next, creationSettings(target), target.mappings(), Set.of());
    return true;
  }

  /** Returns the indices the alias points at, or null when they cannot be read. */
  private Set<String> aliasHolders(String alias) {
    try {
      Set<String> holders = new TreeSet<>();
      engine.getAliases(alias).forEach((index, names) -> {
        if (names.contains(alias)) {
          holders.add(index);
        }
      });
      return holders;
    } catch (RuntimeException e) {
      LOG.warn("{}: could not read the alias: {}", alias, e.getMessage());
      return null;
    }
  }

  /**
   * Cleans up after a failed reindex: lifts the write block on the old index and deletes the new
   * index, but only if this run created it and the alias, read again, does not point at it.
   */
  private void undo(String alias, String current, String next, boolean createdNext) {
    undo(alias, current, next, createdNext, true);
  }

  private void undo(String alias, String current, String next, boolean createdNext,
      boolean unblock) {
    if (unblock) {
      try {
        engine.setWriteBlock(current, false);
      } catch (RuntimeException e) {
        LOG.warn("{}: could not unblock {}: {}", alias, current, e.getMessage());
      }
    }
    if (!createdNext) {
      LOG.warn("{}: keeping {}, which this run reused rather than created", alias, next);
      return;
    }
    Set<String> holders = aliasHolders(alias);
    if (holders == null || holders.contains(next)) {
      LOG.warn("{}: not deleting {}: the alias {}", alias, next,
          holders == null ? "could not be read" : "points at it");
      return;
    }
    LOG.info("{}: deleting {}", alias, next);
    try {
      engine.deleteIndex(next);
    } catch (RuntimeException e) {
      LOG.warn("{}: could not delete {}: {}", alias, next, e.getMessage());
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : Map.of();
  }

  /** Builder for {@link IndexMigrator}. */
  public static final class Builder {

    private SearchEngine engine;
    private ProtoMapper mapper = ProtoMapper.withDefaults();
    private MigrationOptions options = MigrationOptions.standard();
    private MigrationScriptProvider scripts = MigrationScriptProvider.classpath();
    private MigrationListener listener = MigrationListener.noop();

    private Builder() {}

    /** Sets the engine; required. */
    public Builder engine(SearchEngine value) {
      this.engine = value;
      return this;
    }

    /** Sets the mapper; defaults to {@link ProtoMapper#withDefaults()}. */
    public Builder mapper(ProtoMapper value) {
      this.mapper = Objects.requireNonNull(value);
      return this;
    }

    /** Sets the options; defaults to {@link MigrationOptions#standard()}. */
    public Builder options(MigrationOptions value) {
      this.options = Objects.requireNonNull(value);
      return this;
    }

    /** Sets the script provider; defaults to {@link MigrationScriptProvider#classpath()}. */
    public Builder scriptProvider(MigrationScriptProvider value) {
      this.scripts = Objects.requireNonNull(value);
      return this;
    }

    /**
     * Sets the listener told about reindex and backfill phases; defaults to {@link
     * MigrationListener#noop()}.
     */
    public Builder listener(MigrationListener value) {
      this.listener = Objects.requireNonNull(value);
      return this;
    }

    /** Builds the migrator. */
    public IndexMigrator build() {
      return new IndexMigrator(this);
    }
  }
}
