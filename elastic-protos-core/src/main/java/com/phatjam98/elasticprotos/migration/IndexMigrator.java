package com.phatjam98.elasticprotos.migration;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.phatjam98.elasticprotos.engine.AliasAction;
import com.phatjam98.elasticprotos.engine.ReindexRequest;
import com.phatjam98.elasticprotos.engine.ReindexResult;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.mapping.IndexDefinition;
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
 */
public final class IndexMigrator {

  private static final Logger LOG = LoggerFactory.getLogger(IndexMigrator.class);

  /** The hash reported for indices created without elastic-protos metadata. */
  public static final String LEGACY_HASH = "legacy";

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
      if (ProtoMapper.isSkipped(descriptor)) {
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
        if (current.equals(target.name())) {
          return new IndexPlan(target, Action.BLOCKED, current, currentHash, diff,
              "index " + current + " already has the target name but different mappings: "
                  + breaking, warnings);
        }
        return new IndexPlan(target, Action.REINDEX, current, currentHash, diff,
            "breaking change: " + breaking, warnings);
    }
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
          return new Outcome(item, Status.APPLIED, item.currentIndex(), List.of(), null);
        case REINDEX:
          return reindex(item);
        case BLOCKED:
          LOG.info("{}: blocked: {}", item.alias(), item.reason());
          return new Outcome(item, Status.SKIPPED, item.currentIndex(), List.of(), null);
        default:
          LOG.info("{}: {} is up to date", item.alias(), item.currentIndex());
          return new Outcome(item, Status.SKIPPED, item.currentIndex(), List.of(), null);
      }
    } catch (RuntimeException e) {
      LOG.info("{}: {} failed: {}", item.alias(), item.action(), e.getMessage());
      return new Outcome(item, Status.FAILED, item.currentIndex(), List.of(), e);
    }
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

    if (engine.indexExists(next)) {
      LOG.info("{}: reusing existing index {}", alias, next);
    } else {
      LOG.info("{}: creating {} for the reindex from {}", alias, next, current);
      engine.createIndex(next, creationSettings(target), target.mappings(), Set.of());
    }
    LOG.info("{}: blocking writes to {}", alias, current);
    engine.setWriteBlock(current, true);
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
      long before = engine.count(current);
      long after = engine.count(next);
      if (before != after) {
        throw new MigrationException("reindex " + current + " -> " + next + " copied " + after
            + " of " + before + " documents");
      }
    } catch (RuntimeException e) {
      LOG.info("{}: reindex failed, unblocking {} and deleting {}", alias, current, next);
      undo(current, next);
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
      undo(current, next);
      throw new MigrationException("alias swap " + current + " -> " + next + " failed: "
          + e.getMessage(), e);
    }
    if (options.deleteOldAfter()) {
      LOG.info("{}: deleting old index {}", alias, current);
      engine.deleteIndex(current);
      return new Outcome(item, Status.APPLIED, next, List.of(), null);
    }
    LOG.info("{}: retaining old index {} (write-blocked)", alias, current);
    return new Outcome(item, Status.APPLIED, next, List.of(current), null);
  }

  private void undo(String current, String next) {
    try {
      engine.setWriteBlock(current, false);
    } catch (RuntimeException e) {
      LOG.info("could not unblock {}: {}", current, e.getMessage());
    }
    try {
      engine.deleteIndex(next);
    } catch (RuntimeException e) {
      LOG.info("could not delete {}: {}", next, e.getMessage());
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
