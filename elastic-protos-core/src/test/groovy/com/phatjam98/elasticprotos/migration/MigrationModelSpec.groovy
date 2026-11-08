package com.phatjam98.elasticprotos.migration

import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.example.Firm
import java.time.Duration
import spock.lang.Specification

class MigrationModelSpec extends Specification {

  def "standard options"() {
    expect:
    MigrationOptions.standard() == new MigrationOptions(FailurePolicy.FAIL_FAST, true, false,
        RemovedFieldPolicy.KEEP, 1, ReplicaPolicy.dataNodesMinusOne(1), Duration.ofMinutes(10), false)
    MigrationOptions.standard().clearStaleWriteBlock()
  }

  def "with-copies change one component each"() {
    given:
    def base = MigrationOptions.standard()

    expect:
    base.withFailure(FailurePolicy.CONTINUE).failure() == FailurePolicy.CONTINUE
    !base.withAllowReindex(false).allowReindex()
    base.withDeleteOldAfter(true).deleteOldAfter()
    base.withRemovedFields(RemovedFieldPolicy.REINDEX).removedFields() == RemovedFieldPolicy.REINDEX
    base.withDefaultShards(4).defaultShards() == 4
    base.withReplicas(ReplicaPolicy.fixed(2)).replicas() == ReplicaPolicy.fixed(2)
    base.withReindexTimeout(Duration.ofSeconds(5)).reindexTimeout() == Duration.ofSeconds(5)
    base.withDryRun(true).dryRun()
    base.withDryRun(true).withFailure(FailurePolicy.CONTINUE).failure() == FailurePolicy.CONTINUE
    !base.withClearStaleWriteBlock(false).clearStaleWriteBlock()
    !base.withClearStaleWriteBlock(false).withDryRun(true).clearStaleWriteBlock()
  }

  def "options are validated"() {
    when:
    MigrationOptions.standard().withDefaultShards(0)

    then:
    thrown(IllegalArgumentException)

    when:
    MigrationOptions.standard().withReplicas(null)

    then:
    thrown(IllegalArgumentException)
  }

  def "replica policies"() {
    expect:
    policy.replicas(nodes) == replicas
    policy.needsDataNodes() == needsNodes

    where:
    policy                             | nodes | replicas | needsNodes
    ReplicaPolicy.fixed(2)             | 0     | 2        | false
    ReplicaPolicy.dataNodesMinusOne(1) | 0     | 0        | true
    ReplicaPolicy.dataNodesMinusOne(1) | 1     | 0        | true
    ReplicaPolicy.dataNodesMinusOne(1) | 2     | 1        | true
    ReplicaPolicy.dataNodesMinusOne(1) | 9     | 1        | true
    ReplicaPolicy.dataNodesMinusOne(4) | 3     | 2        | true
  }

  def "replica policies are validated"() {
    when:
    new ReplicaPolicy(mode, count)

    then:
    thrown(IllegalArgumentException)

    where:
    mode                        | count
    null                        | 1
    ReplicaPolicy.Mode.FIXED    | -1
  }

  def "result ok() fails on FAILED outcomes and on blocked items"() {
    given:
    def target = ProtoMapper.withDefaults().map(Firm)
    def plan = { Action action -> new IndexPlan(target, action, null, null, MappingDiff.empty(), 'r', []) }
    def outcome = { Action action, MigrationResult.Status status ->
      new MigrationResult.Outcome(plan(action), status, null, [], null)
    }

    expect:
    new MigrationResult([]).ok()
    new MigrationResult([outcome(Action.CREATE, MigrationResult.Status.APPLIED),
                         outcome(Action.NO_OP, MigrationResult.Status.SKIPPED),
                         outcome(Action.REINDEX, MigrationResult.Status.DRY_RUN)]).ok()
    !new MigrationResult([outcome(Action.CREATE, MigrationResult.Status.FAILED)]).ok()
    !new MigrationResult([outcome(Action.BLOCKED, MigrationResult.Status.SKIPPED)]).ok()
    !new MigrationResult([outcome(Action.BLOCKED, MigrationResult.Status.DRY_RUN)]).ok()
    new MigrationResult([outcome(Action.CREATE, MigrationResult.Status.APPLIED),
                         outcome(Action.BLOCKED, MigrationResult.Status.SKIPPED)]).problems().size() == 1
    plan(Action.CREATE).alias() == 'firms'
  }

  def "an empty plan has no changes"() {
    given:
    def plan = new MigrationPlan([])

    expect:
    !plan.hasChanges()
    plan.blocked().isEmpty()
    plan.toReport() == 'Migration plan: 0 indices, no changes\n'
  }

  def "the report prints current -> target for #action and the reason for no-ops"() {
    given:
    def target = ProtoMapper.withDefaults().map(Firm)
    def item = new IndexPlan(target, action, current, null, MappingDiff.empty(), 'why', [])
    String line = new MigrationPlan([item]).toReport().readLines()[1].trim()

    expect:
    line == "${action.name().padRight(11)} firms: ${indices.replace('TARGET', target.name())} (added 0, removed 0, changed 0)".toString()
    new MigrationPlan([item]).toReport().readLines().size() == lines

    where:
    action             | current    | indices                  | lines
    Action.CREATE      | null       | '(none) -> TARGET'       | 2
    Action.REINDEX     | 'firms-v1' | 'firms-v1 -> TARGET'     | 2
    Action.BLOCKED     | null       | '(none) -> TARGET'       | 3
    Action.PUT_MAPPING | 'firms-v1' | 'firms-v1 (in place)'    | 2
    Action.NO_OP       | 'firms-v1' | 'firms-v1 (in place)'    | 3
  }

  def "exceptions carry messages and causes"() {
    given:
    def cause = new IllegalStateException('x')

    expect:
    new MigrationException('m').message == 'm'
    new MigrationException('m', cause).cause.is(cause)
  }
}
