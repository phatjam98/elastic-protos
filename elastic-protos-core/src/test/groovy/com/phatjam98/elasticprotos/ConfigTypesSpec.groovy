package com.phatjam98.elasticprotos

import com.google.protobuf.Timestamp
import com.google.protobuf.TypeRegistry
import com.phatjam98.elasticprotos.codec.CodecConfig
import com.phatjam98.elasticprotos.codec.UnknownFieldPolicy
import com.phatjam98.elasticprotos.engine.ReindexRequest
import com.phatjam98.elasticprotos.mapping.FieldKind
import com.phatjam98.elasticprotos.mapping.MappingDefaults
import com.phatjam98.elasticprotos.migration.FailurePolicy
import com.phatjam98.elasticprotos.migration.MigrationOptions
import com.phatjam98.elasticprotos.migration.RemovedFieldPolicy
import com.phatjam98.elasticprotos.migration.ReplicaPolicy
import com.phatjam98.elasticprotos.proto.DynamicMode
import com.phatjam98.elasticprotos.query.QueryCompilerConfig
import com.phatjam98.elasticprotos.query.TrackTotalHits
import java.time.Duration
import spock.lang.Specification
import spock.lang.Unroll

/** The configuration value types: builders, copies, equality and validation. */
class ConfigTypesSpec extends Specification {

  // ---- MappingDefaults ---------------------------------------------------------------------

  def "MappingDefaults: the builder defaults are the standard defaults"() {
    expect:
    MappingDefaults.builder().build() == MappingDefaults.standard()
    MappingDefaults.builder().build().hashCode() == MappingDefaults.standard().hashCode()
    MappingDefaults.standard().toBuilder().build() == MappingDefaults.standard()
    with(MappingDefaults.standard()) {
      !stringsAsText()
      keywordSubfield()
      ignoreAbove() == 256
      dateFormat() == 'strict_date_optional_time'
      maxDepth() == 5
      dynamic() == DynamicMode.DYNAMIC_STRICT
      singularMessage() == FieldKind.OBJECT
      repeatedMessage() == FieldKind.NESTED
      structKind() == FieldKind.FLATTENED
      maxNestedFields() == MappingDefaults.DEFAULT_MAX_NESTED_FIELDS
      maxTotalFields() == MappingDefaults.DEFAULT_MAX_TOTAL_FIELDS
    }
  }

  def "MappingDefaults: every builder setter matches its with-copy"() {
    given:
    def built = MappingDefaults.builder()
        .stringsAsText(true).keywordSubfield(false).ignoreAbove(512).dateFormat('epoch_millis')
        .maxDepth(3).dynamic(DynamicMode.DYNAMIC_FALSE).singularMessage(FieldKind.NESTED)
        .repeatedMessage(FieldKind.OBJECT).structKind(FieldKind.OBJECT).maxNestedFields(10)
        .maxTotalFields(100).build()
    def copied = MappingDefaults.standard()
        .withStringsAsText(true).withKeywordSubfield(false).withIgnoreAbove(512)
        .withDateFormat('epoch_millis').withMaxDepth(3).withDynamic(DynamicMode.DYNAMIC_FALSE)
        .withSingularMessage(FieldKind.NESTED).withRepeatedMessage(FieldKind.OBJECT)
        .withStructKind(FieldKind.OBJECT).withMaxNestedFields(10).withMaxTotalFields(100)

    expect:
    built == copied
    built.hashCode() == copied.hashCode()
    built.toBuilder().build() == built
    built != MappingDefaults.standard()
    built.toString().startsWith('MappingDefaults[stringsAsText=true, keywordSubfield=false')
    built.toString().endsWith('maxTotalFields=100]')
    !built.equals('x')
  }

  @Unroll
  def "MappingDefaults: #name alone breaks equality"() {
    expect:
    change(MappingDefaults.standard()) != MappingDefaults.standard()

    where:
    name              | change
    'stringsAsText'   | { it.withStringsAsText(true) }
    'keywordSubfield' | { it.withKeywordSubfield(false) }
    'ignoreAbove'     | { it.withIgnoreAbove(1) }
    'dateFormat'      | { it.withDateFormat('x') }
    'maxDepth'        | { it.withMaxDepth(1) }
    'dynamic'         | { it.withDynamic(DynamicMode.DYNAMIC_TRUE) }
    'singularMessage' | { it.withSingularMessage(FieldKind.NESTED) }
    'repeatedMessage' | { it.withRepeatedMessage(FieldKind.OBJECT) }
    'structKind'      | { it.withStructKind(FieldKind.OBJECT) }
    'maxNestedFields' | { it.withMaxNestedFields(1) }
    'maxTotalFields'  | { it.withMaxTotalFields(1) }
  }

  @Unroll
  def "MappingDefaults: rejects #name"() {
    when:
    change(MappingDefaults.builder()).build()

    then:
    def e = thrown(exception)
    e.message == message

    where:
    name                    | change                                      || exception                | message
    'null dateFormat'       | { it.dateFormat(null) }                     || NullPointerException     | 'dateFormat'
    'null dynamic'          | { it.dynamic(null) }                        || NullPointerException     | 'dynamic'
    'null singularMessage'  | { it.singularMessage(null) }                || NullPointerException     | 'singularMessage'
    'null repeatedMessage'  | { it.repeatedMessage(null) }                || NullPointerException     | 'repeatedMessage'
    'null structKind'       | { it.structKind(null) }                     || NullPointerException     | 'structKind'
    'keyword singulars'     | { it.singularMessage(FieldKind.KEYWORD) }   || IllegalArgumentException | 'singularMessage must be OBJECT or NESTED: KEYWORD'
    'flattened repeateds'   | { it.repeatedMessage(FieldKind.FLATTENED) } || IllegalArgumentException | 'repeatedMessage must be OBJECT or NESTED: FLATTENED'
    'nested structs'        | { it.structKind(FieldKind.NESTED) }         || IllegalArgumentException | 'structKind must be FLATTENED or OBJECT: NESTED'
    'negative ignoreAbove'  | { it.ignoreAbove(-1) }                      || IllegalArgumentException | 'ignoreAbove must not be negative: -1'
    'negative maxDepth'     | { it.maxDepth(-1) }                         || IllegalArgumentException | 'maxDepth must not be negative: -1'
    'negative nested limit' | { it.maxNestedFields(-1) }                  || IllegalArgumentException | 'maxNestedFields must not be negative: -1'
    'negative total limit'  | { it.maxTotalFields(-1) }                   || IllegalArgumentException | 'maxTotalFields must not be negative: -1'
  }

  def "MappingDefaults: zero limits and the other allowed kinds are accepted"() {
    expect:
    MappingDefaults.builder().ignoreAbove(0).maxDepth(0).maxNestedFields(0).maxTotalFields(0)
        .singularMessage(FieldKind.NESTED).repeatedMessage(FieldKind.OBJECT).structKind(FieldKind.OBJECT)
        .build().maxDepth() == 0
  }

  @Unroll
  def "CodecConfig: rejects a null #name"() {
    when:
    change(CodecConfig.builder()).build()

    then:
    def e = thrown(NullPointerException)
    e.message == name

    where:
    name            | change
    'unknownFields' | { it.unknownFields(null) }
    'anyTypes'      | { it.anyTypes(null) }
  }

  def "CodecConfig: type registries compare by identity"() {
    given:
    def a = TypeRegistry.newBuilder().add(Timestamp.descriptor).build()
    def b = TypeRegistry.newBuilder().add(Timestamp.descriptor).build()

    expect:
    CodecConfig.standard().withAnyTypes(a) == CodecConfig.standard().withAnyTypes(a)
    CodecConfig.standard().withAnyTypes(a) != CodecConfig.standard().withAnyTypes(b)
  }

  // ---- CodecConfig -------------------------------------------------------------------------

  def "CodecConfig: builder, copies and equality"() {
    given:
    def registry = TypeRegistry.newBuilder().add(Timestamp.descriptor).build()
    def built = CodecConfig.builder().includeDefaults(false).int64AsNumber(false)
        .unknownFields(UnknownFieldPolicy.FAIL).anyTypes(registry).build()

    expect:
    CodecConfig.builder().build() == CodecConfig.standard()
    CodecConfig.standard().toBuilder().build() == CodecConfig.standard()
    CodecConfig.standard().hashCode() == CodecConfig.builder().build().hashCode()
    with(CodecConfig.standard()) {
      includeDefaults()
      int64AsNumber()
      unknownFields() == UnknownFieldPolicy.IGNORE
      anyTypes() == TypeRegistry.emptyTypeRegistry
    }
    built == CodecConfig.standard().withIncludeDefaults(false).withInt64AsNumber(false)
        .withUnknownFields(UnknownFieldPolicy.FAIL).withAnyTypes(registry)
    built.toBuilder().build() == built
    built.anyTypes().is(registry)
    CodecConfig.standard().withIncludeDefaults(false) != CodecConfig.standard()
    CodecConfig.standard().withInt64AsNumber(false) != CodecConfig.standard()
    CodecConfig.standard().withUnknownFields(UnknownFieldPolicy.FAIL) != CodecConfig.standard()
    CodecConfig.standard().withAnyTypes(registry) != CodecConfig.standard()
    built.toString().startsWith('CodecConfig[includeDefaults=false, int64AsNumber=false, unknownFields=FAIL')
    !built.equals(null)
  }

  // ---- QueryCompilerConfig -----------------------------------------------------------------

  def "QueryCompilerConfig: builder, copies and equality"() {
    given:
    def built = QueryCompilerConfig.builder()
        .allowLeadingWildcards(true).lenient(true).trackTotalHits(TrackTotalHits.upTo(500))
        .maxResultWindow(50_000).defaultSize(25).defaultKeepAlive(Duration.ofMinutes(5))
        .groupNested(false).shardDocTiebreaker(false).allowScriptSort(true).allowRegex(true)
        .maxPageSize(1_000).build()
    def copied = QueryCompilerConfig.standard()
        .withAllowLeadingWildcards(true).withLenient(true)
        .withTrackTotalHits(TrackTotalHits.upTo(500)).withMaxResultWindow(50_000)
        .withDefaultSize(25).withDefaultKeepAlive(Duration.ofMinutes(5)).withGroupNested(false)
        .withShardDocTiebreaker(false).withAllowScriptSort(true).withAllowRegex(true)
        .withMaxPageSize(1_000)

    expect:
    QueryCompilerConfig.builder().build() == QueryCompilerConfig.standard()
    QueryCompilerConfig.standard().toBuilder().build() == QueryCompilerConfig.standard()
    QueryCompilerConfig.standard().hashCode() == QueryCompilerConfig.builder().build().hashCode()
    built == copied
    built.hashCode() == copied.hashCode()
    built.toBuilder().build() == built
    built.trackTotalHits() == TrackTotalHits.upTo(500)
    built.toString() == 'QueryCompilerConfig[allowLeadingWildcards=true, lenient=true,' +
        ' trackTotalHits=500, maxResultWindow=50000, defaultSize=25, defaultKeepAlive=PT5M,' +
        ' groupNested=false, shardDocTiebreaker=false, allowScriptSort=true, allowRegex=true,' +
        ' maxPageSize=1000]'
    !built.equals('x')
  }

  def "QueryCompilerConfig: the builder validates only the final settings"() {
    expect: 'a cap below the default size is fine when the default size is lowered too'
    QueryCompilerConfig.builder().maxPageSize(5).defaultSize(5).build().maxPageSize() == 5
  }

  @Unroll
  def "QueryCompilerConfig: #name breaks equality"() {
    expect:
    change(QueryCompilerConfig.standard()) != QueryCompilerConfig.standard()

    where:
    name                    | change
    'allowLeadingWildcards' | { it.withAllowLeadingWildcards(true) }
    'lenient'               | { it.withLenient(true) }
    'trackTotalHits'        | { it.withTrackTotalHits(TrackTotalHits.none()) }
    'engine default totals' | { it.withTrackTotalHits(TrackTotalHits.engineDefault()) }
    'maxResultWindow'       | { it.withMaxResultWindow(1) }
    'defaultSize'           | { it.withDefaultSize(1) }
    'defaultKeepAlive'      | { it.withDefaultKeepAlive(Duration.ofSeconds(1)) }
    'groupNested'           | { it.withGroupNested(false) }
    'shardDocTiebreaker'    | { it.withShardDocTiebreaker(false) }
    'allowScriptSort'       | { it.withAllowScriptSort(true) }
    'allowRegex'            | { it.withAllowRegex(true) }
    'maxPageSize'           | { it.withMaxPageSize(11) }
  }

  @Unroll
  def "QueryCompilerConfig: rejects #name with '#message'"() {
    when:
    change(QueryCompilerConfig.builder()).build()

    then:
    def e = thrown(exception)
    e.message == message

    where:
    name               | change                                             || exception                | message
    'zero window'      | { it.maxResultWindow(0) }                          || IllegalArgumentException | 'maxResultWindow must be positive: 0'
    'negative size'    | { it.defaultSize(-1) }                             || IllegalArgumentException | 'defaultSize must not be negative: -1'
    'zero page cap'    | { it.maxPageSize(0) }                              || IllegalArgumentException | 'maxPageSize must be positive: 0'
    'size above cap'   | { it.maxPageSize(9) }                              || IllegalArgumentException | 'defaultSize 10 must not exceed maxPageSize 9'
    'null keep-alive'  | { it.defaultKeepAlive(null) }                      || NullPointerException     | 'defaultKeepAlive'
    'zero keep-alive'  | { it.defaultKeepAlive(Duration.ZERO) }             || IllegalArgumentException | 'defaultKeepAlive must be positive: PT0S'
    'neg. keep-alive'  | { it.defaultKeepAlive(Duration.ofSeconds(-1)) }    || IllegalArgumentException | 'defaultKeepAlive must be positive: PT-1S'
    'null totals'      | { it.trackTotalHits(null) }                        || NullPointerException     | 'trackTotalHits'
  }

  // ---- TrackTotalHits ----------------------------------------------------------------------

  def "TrackTotalHits serialises to true, false, the bound, or nothing for the engine default"() {
    expect:
    TrackTotalHits.engineDefault().toJson() == null
    TrackTotalHits.engineDefault().kind() == TrackTotalHits.Kind.DEFAULT
    TrackTotalHits.engineDefault().toString() == 'default'
    TrackTotalHits.engineDefault() != TrackTotalHits.exact()
    TrackTotalHits.exact().toJson() == Boolean.TRUE
    TrackTotalHits.none().toJson() == Boolean.FALSE
    TrackTotalHits.upTo(1000).toJson() instanceof Integer
    TrackTotalHits.upTo(1000).toJson() == 1000
    TrackTotalHits.exact().kind() == TrackTotalHits.Kind.EXACT
    TrackTotalHits.none().kind() == TrackTotalHits.Kind.NONE
    TrackTotalHits.upTo(7).kind() == TrackTotalHits.Kind.UP_TO
    TrackTotalHits.upTo(7).limit() == 7
    TrackTotalHits.exact().limit() == 0
    TrackTotalHits.of(true) == TrackTotalHits.exact()
    TrackTotalHits.of(false) == TrackTotalHits.none()
    TrackTotalHits.upTo(7) == TrackTotalHits.upTo(7)
    TrackTotalHits.upTo(7).hashCode() == TrackTotalHits.upTo(7).hashCode()
    TrackTotalHits.upTo(7) != TrackTotalHits.upTo(8)
    TrackTotalHits.upTo(0) != TrackTotalHits.exact()
    !TrackTotalHits.exact().equals(true)
    TrackTotalHits.exact().toString() == 'true'
    TrackTotalHits.none().toString() == 'false'
    TrackTotalHits.upTo(42).toString() == '42'
    TrackTotalHits.upTo(Integer.MAX_VALUE).limit() == Integer.MAX_VALUE
  }

  @Unroll
  def "TrackTotalHits parses '#text'"() {
    expect:
    TrackTotalHits.parse(text) == expected

    where:
    text      || expected
    'true'    || TrackTotalHits.exact()
    ' TRUE '  || TrackTotalHits.exact()
    'false'   || TrackTotalHits.none()
    'False'   || TrackTotalHits.none()
    '0'       || TrackTotalHits.upTo(0)
    '10000'   || TrackTotalHits.upTo(10000)
    'Default' || TrackTotalHits.engineDefault()
    '2147483647' || TrackTotalHits.upTo(Integer.MAX_VALUE)
  }

  @Unroll
  def "TrackTotalHits rejects #description"() {
    when:
    action()

    then:
    def e = thrown(IllegalArgumentException)
    e.message == message

    where:
    description     | action                                            || message
    'a word'          | { TrackTotalHits.parse('yes') }                    || 'trackTotalHits must be true, false, default or a number, got yes'
    'a plus sign'     | { TrackTotalHits.parse('+5') }                     || 'trackTotalHits must be true, false, default or a number, got +5'
    'a decimal'       | { TrackTotalHits.parse('1.5') }                    || 'trackTotalHits must be true, false, default or a number, got 1.5'
    'a negative text' | { TrackTotalHits.parse('-1') }                     || 'trackTotalHits limit must not be negative: -1'
    'a negative'      | { TrackTotalHits.upTo(-1) }                        || 'trackTotalHits limit must not be negative: -1'
    'beyond int'      | { TrackTotalHits.parse('2147483648') }             || 'trackTotalHits limit out of range (0 to 2147483647): 2147483648'
    'beyond long'     | { TrackTotalHits.parse('99999999999999999999') }   || 'trackTotalHits limit out of range (0 to 2147483647): 99999999999999999999'
  }

  def "TrackTotalHits.parse rejects null"() {
    when:
    TrackTotalHits.parse(null)

    then:
    thrown(NullPointerException)
  }

  // ---- MigrationOptions --------------------------------------------------------------------

  def "MigrationOptions: builder, copies and equality"() {
    given:
    def built = MigrationOptions.builder()
        .failure(FailurePolicy.CONTINUE).allowReindex(false).deleteOldAfter(true)
        .removedFields(RemovedFieldPolicy.REINDEX).defaultShards(3).replicas(ReplicaPolicy.fixed(2))
        .reindexTimeout(Duration.ofSeconds(90)).dryRun(true).clearStaleWriteBlock(false).build()
    def copied = MigrationOptions.standard()
        .withFailure(FailurePolicy.CONTINUE).withAllowReindex(false).withDeleteOldAfter(true)
        .withRemovedFields(RemovedFieldPolicy.REINDEX).withDefaultShards(3)
        .withReplicas(ReplicaPolicy.fixed(2)).withReindexTimeout(Duration.ofSeconds(90))
        .withDryRun(true).withClearStaleWriteBlock(false)

    expect:
    MigrationOptions.builder().build() == MigrationOptions.standard()
    MigrationOptions.standard().toBuilder().build() == MigrationOptions.standard()
    MigrationOptions.standard().hashCode() == MigrationOptions.builder().build().hashCode()
    built == copied
    built.hashCode() == copied.hashCode()
    built.toBuilder().build() == built
    built.toString() == 'MigrationOptions[failure=CONTINUE, allowReindex=false, deleteOldAfter=true,' +
        ' removedFields=REINDEX, defaultShards=3, replicas=ReplicaPolicy[mode=FIXED, count=2],' +
        ' reindexTimeout=PT1M30S, dryRun=true, clearStaleWriteBlock=false]'
    !built.equals('x')
  }

  @Unroll
  def "MigrationOptions: #name breaks equality"() {
    expect:
    change(MigrationOptions.standard()) != MigrationOptions.standard()

    where:
    name                   | change
    'failure'              | { it.withFailure(FailurePolicy.CONTINUE) }
    'allowReindex'         | { it.withAllowReindex(false) }
    'deleteOldAfter'       | { it.withDeleteOldAfter(true) }
    'removedFields'        | { it.withRemovedFields(RemovedFieldPolicy.REINDEX) }
    'defaultShards'        | { it.withDefaultShards(2) }
    'replicas'             | { it.withReplicas(ReplicaPolicy.fixed(1)) }
    'reindexTimeout'       | { it.withReindexTimeout(Duration.ofSeconds(1)) }
    'dryRun'               | { it.withDryRun(true) }
    'clearStaleWriteBlock' | { it.withClearStaleWriteBlock(false) }
  }

  @Unroll
  def "MigrationOptions: rejects #name"() {
    when:
    change(MigrationOptions.builder()).build()

    then:
    def e = thrown(exception)
    e.message == message

    where:
    name             | change                                       || exception                | message
    'zero shards'    | { it.defaultShards(0) }                      || IllegalArgumentException | 'defaultShards must be at least 1: 0'
    'zero timeout'   | { it.reindexTimeout(Duration.ZERO) }         || IllegalArgumentException | 'reindexTimeout must be positive: PT0S'
    'neg. timeout'   | { it.reindexTimeout(Duration.ofSeconds(-1)) } || IllegalArgumentException | 'reindexTimeout must be positive: PT-1S'
    'no failure'     | { it.failure(null) }                         || NullPointerException     | 'failure'
    'no removed'     | { it.removedFields(null) }                   || NullPointerException     | 'removedFields'
    'no replicas'    | { it.replicas(null) }                        || NullPointerException     | 'replicas'
    'no timeout'     | { it.reindexTimeout(null) }                  || NullPointerException     | 'reindexTimeout'
  }

  // ---- ReplicaPolicy -----------------------------------------------------------------------

  def "ReplicaPolicy: factories, equality and validation"() {
    expect:
    ReplicaPolicy.fixed(2) == ReplicaPolicy.fixed(2)
    ReplicaPolicy.fixed(2).hashCode() == ReplicaPolicy.fixed(2).hashCode()
    ReplicaPolicy.fixed(2) != ReplicaPolicy.fixed(1)
    ReplicaPolicy.fixed(1) != ReplicaPolicy.dataNodesMinusOne(1)
    !ReplicaPolicy.fixed(1).equals(1)
    ReplicaPolicy.fixed(2).mode() == ReplicaPolicy.Mode.FIXED
    ReplicaPolicy.fixed(2).count() == 2
    ReplicaPolicy.dataNodesMinusOne(3).mode() == ReplicaPolicy.Mode.DATA_NODES_MINUS_ONE
    ReplicaPolicy.dataNodesMinusOne(3).toString() == 'ReplicaPolicy[mode=DATA_NODES_MINUS_ONE, count=3]'

    when:
    ReplicaPolicy.dataNodesMinusOne(-2)

    then:
    def e = thrown(IllegalArgumentException)
    e.message == 'replica count must not be negative: -2'
  }

  // ---- ReindexRequest ----------------------------------------------------------------------

  def "ReindexRequest: of, builder, copies and equality"() {
    given:
    def plain = ReindexRequest.of('a', 'b')
    def built = ReindexRequest.builder().source('a').dest('b').script('s', [x: 1])
        .timeout(Duration.ofMinutes(1)).refresh(false).build()

    expect:
    plain == ReindexRequest.builder().source('a').dest('b').build()
    plain.hashCode() == ReindexRequest.builder().source('a').dest('b').build().hashCode()
    plain.toBuilder().build() == plain
    plain.script() == null
    plain.scriptParams() == [:]
    plain.timeout() == Duration.ofMinutes(10)
    plain.refresh()
    built.toBuilder().build() == built
    built.script() == 's'
    built.scriptParams() == [x: 1]
    !built.refresh()
    plain.withScript('s', [x: 1]).withTimeout(Duration.ofMinutes(1)) ==
        built.toBuilder().refresh(true).build()
    built != plain
    plain != ReindexRequest.of('a', 'c')
    plain != ReindexRequest.of('c', 'b')
    plain != plain.withTimeout(Duration.ofMinutes(1))
    plain != plain.toBuilder().script('s').build()
    plain != plain.toBuilder().scriptParams([y: 2]).build()
    plain.withSource('c') == ReindexRequest.of('c', 'b')
    plain.withDest('c') == ReindexRequest.of('a', 'c')
    !plain.withRefresh(false).refresh()
    plain.withScriptParams([y: 2]).scriptParams() == [y: 2]
    plain.withScriptParams([y: 2]).script() == null
    !plain.equals('x')
    plain.toString() == 'ReindexRequest[source=a, dest=b, script=null, scriptParams={}, timeout=PT10M, refresh=true]'
  }

  def "ReindexRequest: script params are copied, may hold null and cannot be changed"() {
    given:
    def params = [a: 1, b: null]
    def request = ReindexRequest.of('a', 'b').withScript('s', params)

    when:
    params.a = 2
    params.c = 3

    then:
    request.scriptParams() == [a: 1, b: null]

    when:
    request.scriptParams().put('d', 4)

    then:
    thrown(UnsupportedOperationException)
  }

  def "ReindexRequest: a null script clears the script parameters"() {
    given:
    def scripted = ReindexRequest.of('a', 'b').withScript('s', [x: 1])

    expect:
    scripted.toBuilder().script(null).build() == ReindexRequest.of('a', 'b')
    scripted.withScript(null, [x: 1]) == ReindexRequest.of('a', 'b')
    scripted.toBuilder().script('t').build().scriptParams() == [x: 1]
  }

  @Unroll
  def "ReindexRequest: rejects #name"() {
    when:
    builder.build()

    then:
    def e = thrown(exception)
    e.message == message

    where:
    name               | builder                                                               || exception                | message
    'a missing source' | ReindexRequest.builder().dest('b')                                    || NullPointerException     | 'source'
    'a missing dest'   | ReindexRequest.builder().source('a')                                  || NullPointerException     | 'dest'
    'null params'      | ReindexRequest.builder().source('a').dest('b').scriptParams(null)     || NullPointerException     | 'scriptParams'
    'a null timeout'   | ReindexRequest.builder().source('a').dest('b').timeout(null)          || NullPointerException     | 'timeout'
    'a zero timeout'   | ReindexRequest.builder().source('a').dest('b').timeout(Duration.ZERO) || IllegalArgumentException | 'timeout must be positive: PT0S'
  }
}
