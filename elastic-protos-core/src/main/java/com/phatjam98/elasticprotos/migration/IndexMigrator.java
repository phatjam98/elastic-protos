package com.phatjam98.elasticprotos.migration;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.phatjam98.elasticprotos.engine.AliasAction;
import com.phatjam98.elasticprotos.engine.ReindexRequest;
import com.phatjam98.elasticprotos.engine.ReindexResult;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
import com.phatjam98.elasticprotos.mapping.IndexNamingStrategy;
import com.phatjam98.elasticprotos.mapping.ProtoMapper;
import com.phatjam98.elasticprotos.migration.MappingDiff.Change;
import com.phatjam98.elasticprotos.migration.MigrationResult.Outcome;
import com.phatjam98.elasticprotos.migration.MigrationResult.Status;
import java.util.ArrayList;
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
 * <p>A reindex blocks writes to the old index from the moment the copy starts until the alias has
 * been swapped, so writes fail for that window; zero-downtime dual writes are out of scope. The
 * old index is retained, still write-blocked, unless {@link MigrationOptions#deleteOldAfter()};
 * {@link #rollback(IndexPlan, String)} points the alias back at it.
 *
 * <p>When a reindex fails, the old index is unblocked and the new index is deleted, but only if
 * this run created it and the alias does not point at it. An index that already has the target
 * name when a reindex starts is a leftover from an interrupted run: it is reused when its recorded
 * hash is the target hash and recreated otherwise. A write block found on the index an alias
 * points at is assumed to be left by an interrupted reindex and is cleared when that index needs
 * no reindex, unless {@link MigrationOptions#clearStaleWriteBlock()} is false.
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

  private final SearchEngine engine;
  private final ProtoMapper mapper;
  private final MigrationOptions options;
  private final MigrationScriptProvider scripts;

  private IndexMigrator(Builder builder) {
    this.engine = Objects.requireNonNull(builder.engine, "engine is required");
    this.mapper = builder.mapper;
    this.options = builder.options;
    this.scripts = builder.scripts;
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
    switch (plan.action()) {
      case NO_OP:
      case PUT_MAPPING:
        warning += options.clearStaleWriteBlock()
            ? "it will be cleared (clearStaleWriteBlock)"
            : "it is left in place (clearStaleWriteBlock is false) and writes through "
                + plan.alias() + " fail until it is cleared";
        break;
      case REINDEX:
        warning += "the reindex moves the alias to an unblocked index";
        break;
      default:
        warning += "writes through " + plan.alias() + " fail until it is cleared";
        break;
    }
    List<String> warnings = new ArrayList<>(plan.warnings());
    warnings.add(warning);
    return new IndexPlan(plan.target(), plan.action(), plan.currentIndex(), plan.currentHash(),
        plan.diff(), plan.reason(), warnings, true);
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
    Map<String, Object> settings = engine.getSettings(current);
    IndexPlan plan = planExisting(target, current, mappings, settings);
    return isWriteBlocked(settings) ? withWriteBlock(plan) : plan;
  }

  private IndexPlan planExisting(IndexDefinition target, String current,
      Map<String, Object> mappings, Map<String, Object> settings) {
    String currentHash = recordedHash(mappings);
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

    boolean upToDate = target.contentHash().equals(currentHash);
    if ((upToDate || current.equals(target.name())) && !diff.hasFieldChanges()) {
      return new IndexPlan(target, Action.NO_OP, current, currentHash, diff,
          upToDate ? upToDate(currentHash) : "index " + current + " is up to date", warnings);
    }
    Action action = DiffClassifier.classify(diff, options.removedFields());
    switch (action) {
      case NO_OP:
        return new IndexPlan(target, action, current, currentHash, diff,
            upToDate ? upToDate(currentHash) : "mappings of " + current + " already match",
            warnings);
      case PUT_MAPPING:
        return new IndexPlan(target, action, current, currentHash, diff,
            "compatible change: added " + paths(diff.added()), warnings);
      default:
        String breaking = describeBreaking(diff);
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
        return new IndexPlan(next.equals(target.name()) ? target : renamed(target, next),
            Action.REINDEX, current, currentHash, diff, reason, warnings);
    }
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
        target.fullTextFields(), target.ignoredPaths(), target.idField(), target.warnings(),
        target.dynamicFields());
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
   * Points an alias back at the index it used before a reindex and unblocks writes to it.
   *
   * @param plan the reindex item that was applied
   * @param previousIndex the index to restore, usually {@link IndexPlan#currentIndex()}
   */
  public void rollback(IndexPlan plan, String previousIndex) {
    String alias = plan.alias();
    LOG.info("rolling back {}: alias from {} to {}", alias, plan.target().name(), previousIndex);
    engine.updateAliases(List.of(
        AliasAction.remove(plan.target().name(), alias),
        AliasAction.add(previousIndex, alias)));
    engine.setWriteBlock(previousIndex, false);
  }

  /**
   * Rolls back a reindex to the index the plan found, {@link IndexPlan#currentIndex()}.
   *
   * @param plan the reindex item that was applied
   */
  public void rollback(IndexPlan plan) {
    if (plan.currentIndex() == null) {
      throw new MigrationException("plan for " + plan.alias() + " has no previous index");
    }
    rollback(plan, plan.currentIndex());
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
    } catch (RuntimeException e) {
      LOG.info("{}: {} failed: {}", item.alias(), item.action(), e.getMessage());
      return new Outcome(item, Status.FAILED, item.currentIndex(), List.of(), e);
    }
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
    IndexDefinition target = item.target();
    String alias = target.alias();
    String current = item.currentIndex();
    String next = target.name();

    final boolean createdNext = prepareTarget(target, current);
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
    if (options.deleteOldAfter()) {
      LOG.info("{}: deleting old index {}", alias, current);
      engine.deleteIndex(current);
      return new Outcome(item, Status.APPLIED, next, List.of(), null, before, after, warnings);
    }
    LOG.info("{}: retaining old index {} (write-blocked)", alias, current);
    return new Outcome(item, Status.APPLIED, next, List.of(current), null, before, after,
        warnings);
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
    try {
      engine.setWriteBlock(current, false);
    } catch (RuntimeException e) {
      LOG.warn("{}: could not unblock {}: {}", alias, current, e.getMessage());
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

    /** Builds the migrator. */
    public IndexMigrator build() {
      return new IndexMigrator(this);
    }
  }
}
