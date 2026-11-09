package com.phatjam98.elasticprotos.migration

import com.phatjam98.elasticprotos.migration.MappingDiff.Change
import spock.lang.Specification
import spock.lang.Unroll

class DiffClassifierSpec extends Specification {

  static Map root(Map properties, Map extra = [:]) {
    [dynamic: 'strict', properties: properties] + extra
  }

  static final Map TEXT = [type: 'text', analyzer: 'english']
  static final Map KEYWORD = [type: 'keyword', ignore_above: 256]
  static final Map DYN = [type: 'object', dynamic: 'true']
  static final Map NESTED_DYN = [type: 'nested', dynamic: true]

  @Unroll
  def "#description"() {
    when:
    MappingDiff diff = DiffClassifier.diff(existing, desired, [:], [:])

    then:
    diff.added()*.path == added
    diff.removed()*.path == removed
    diff.typeChanged()*.path == typeChanged
    diff.paramChanged()*.path == paramChanged
    DiffClassifier.classify(diff, RemovedFieldPolicy.KEEP) == action

    where:
    description                                 | existing                                                     | desired                                                                 | added            | removed | typeChanged | paramChanged         | action
    'identical'                                 | root([a: KEYWORD])                                           | root([a: KEYWORD])                                                      | []               | []      | []          | []                   | Action.NO_OP
    '_meta is ignored'                          | root([a: KEYWORD], [_meta: [x: 1]])                          | root([a: KEYWORD], [_meta: [x: 2]])                                     | []               | []      | []          | []                   | Action.NO_OP
    'missing type: object is object'            | root([o: [properties: [a: KEYWORD]]])                        | root([o: [type: 'object', properties: [a: KEYWORD]]])                   | []               | []      | []          | []                   | Action.NO_OP
    'dynamic boolean equals string'             | root([o: [dynamic: 'true']], [dynamic: 'false'])             | root([o: [type: 'object', dynamic: true]], [dynamic: false])            | []               | []      | []          | []                   | Action.NO_OP
    'numbers compare by string'                 | root([a: [type: 'keyword', ignore_above: '256']])            | root([a: KEYWORD])                                                      | []               | []      | []          | []                   | Action.NO_OP
    'single copy_to equals a list'              | root([a: [type: 'keyword', copy_to: 'all']])                 | root([a: [type: 'keyword', copy_to: ['all']]])                          | []               | []      | []          | []                   | Action.NO_OP
    'new field'                                 | root([a: KEYWORD])                                           | root([a: KEYWORD, b: KEYWORD])                                          | ['b']            | []      | []          | []                   | Action.PUT_MAPPING
    'new sub-property'                          | root([o: [properties: [a: KEYWORD]]])                        | root([o: [type: 'object', properties: [a: KEYWORD, b: KEYWORD]]])       | ['o.b']          | []      | []          | []                   | Action.PUT_MAPPING
    'new multi-field'                           | root([t: TEXT])                                              | root([t: TEXT + [fields: [keyword: KEYWORD]]])                          | ['t.keyword']    | []      | []          | []                   | Action.BACKFILL
    'dynamic change'                            | root([a: KEYWORD])                                           | root([a: KEYWORD], [dynamic: 'false'])                                  | []               | []      | []          | ['<root>:dynamic']   | Action.PUT_MAPPING
    'type change'                               | root([t: TEXT])                                              | root([t: KEYWORD])                                                      | []               | []      | ['t']       | []                   | Action.REINDEX
    'object to nested'                          | root([o: [properties: [a: KEYWORD]]])                        | root([o: [type: 'nested', properties: [a: KEYWORD]]])                   | []               | []      | ['o']       | []                   | Action.REINDEX
    'analyzer change'                           | root([t: TEXT])                                              | root([t: [type: 'text', analyzer: 'standard']])                         | []               | []      | []          | ['t:analyzer']       | Action.REINDEX
    'ignore_above change on a multi-field'      | root([t: TEXT + [fields: [keyword: KEYWORD]]])               | root([t: TEXT + [fields: [keyword: [type: 'keyword', ignore_above: 10]]]]) | []            | []      | []          | ['t.keyword:ignore_above'] | Action.REINDEX
    'index disabled'                            | root([a: KEYWORD])                                           | root([a: KEYWORD + [index: false]])                                     | []               | []      | []          | ['a:index']          | Action.REINDEX
    'additions under a changed field'           | root([o: [properties: [a: KEYWORD], dynamic: 'false']])      | root([o: [type: 'object', enabled: false, properties: [a: KEYWORD, b: KEYWORD]]]) | ['o.b'] | []   | []          | ['o:dynamic', 'o:enabled'] | Action.REINDEX
    'removed field is kept'                     | root([a: KEYWORD, b: KEYWORD])                               | root([a: KEYWORD])                                                      | []               | ['b']   | []          | []                   | Action.NO_OP
    'removed field with an addition'            | root([a: KEYWORD, b: KEYWORD])                               | root([a: KEYWORD, c: KEYWORD])                                          | ['c']            | ['b']   | []          | []                   | Action.PUT_MAPPING
    'dynamic object: picked-up keys ignored'    | root([m: DYN + [properties: [tier: TEXT + [fields: [keyword: KEYWORD]]]]]) | root([m: [type: 'object', dynamic: true]])                   | []               | []      | []          | []                   | Action.NO_OP
    'dynamic object: desired children ignored'  | root([m: DYN])                                               | root([m: DYN + [properties: [a: KEYWORD]]])                             | []               | []      | []          | []                   | Action.NO_OP
    'dynamic nested: children ignored'          | root([n: NESTED_DYN + [properties: [a: KEYWORD]]])          | root([n: NESTED_DYN + [properties: [b: KEYWORD]]])                      | []               | []      | []          | []                   | Action.NO_OP
    'dynamic true on the desired side only'     | root([o: [properties: [a: KEYWORD]]])                        | root([o: [type: 'object', dynamic: 'true']])                            | []               | []      | []          | ['o:dynamic']        | Action.PUT_MAPPING
    'dynamic object: own params compared'       | root([m: DYN + [properties: [a: KEYWORD]]])                  | root([m: DYN + [enabled: false]])                                       | []               | []      | []          | ['m:enabled']        | Action.REINDEX
    'dynamic object: type change detected'      | root([m: DYN + [properties: [a: KEYWORD]]])                  | root([m: KEYWORD])                                                      | []               | []      | ['m']       | []                   | Action.REINDEX
    'flattened: sub-properties ignored'         | root([f: [type: 'flattened', properties: [a: KEYWORD]]])     | root([f: [type: 'flattened']])                                          | []               | []      | []          | []                   | Action.NO_OP
    'flat_object: sub-properties ignored'       | root([f: [type: 'flat_object', properties: [a: KEYWORD]]])   | root([f: [type: 'flat_object', properties: [b: KEYWORD]]])              | []               | []      | []          | []                   | Action.NO_OP
    'flattened: own params compared'            | root([f: [type: 'flattened', depth_limit: 20]])              | root([f: [type: 'flattened']])                                          | []               | []      | []          | ['f:depth_limit']    | Action.REINDEX
    'dynamic false: children still compared'    | root([o: [dynamic: 'false', properties: [a: KEYWORD, b: KEYWORD]]]) | root([o: [type: 'object', dynamic: false, properties: [a: KEYWORD]]]) | []         | ['o.b'] | []          | []                   | Action.NO_OP
    'dynamic strict: children still compared'   | root([o: [dynamic: 'strict', properties: [a: KEYWORD]]])     | root([o: [type: 'object', dynamic: 'strict', properties: [a: KEYWORD, b: KEYWORD]]]) | ['o.b'] | []    | []          | []                   | Action.PUT_MAPPING
  }

  @Unroll
  def "additions: #description"() {
    when:
    MappingDiff diff = DiffClassifier.diff(existing, desired, [:], [:])

    then:
    diff.added()*.path == added
    diff.unindexedAdditions()*.path == unindexed
    diff.removed().isEmpty()
    diff.paramChanged()*.path == paramChanged
    DiffClassifier.classify(diff, RemovedFieldPolicy.REINDEX) == action

    where:
    description                                     | existing                                                    | desired                                                                                | added             | unindexed         | paramChanged  | action
    'multi-field under an existing field'           | root([t: TEXT])                                             | root([t: TEXT + [fields: [keyword: KEYWORD]]])                                         | ['t.keyword']     | ['t.keyword']     | []            | Action.BACKFILL
    'second multi-field next to an existing one'    | root([t: TEXT + [fields: [keyword: KEYWORD]]])              | root([t: TEXT + [fields: [keyword: KEYWORD, raw: KEYWORD]]])                           | ['t.raw']         | ['t.raw']         | []            | Action.BACKFILL
    'multi-field under a nested property'           | root([n: [type: 'nested', properties: [t: TEXT]]])          | root([n: [type: 'nested', properties: [t: TEXT + [fields: [keyword: KEYWORD]]]]])      | ['n.t.keyword']   | ['n.t.keyword']   | []            | Action.BACKFILL
    'new field that carries multi-fields'           | root([a: KEYWORD])                                          | root([a: KEYWORD, t: TEXT + [fields: [keyword: KEYWORD]]])                             | ['t']             | []                | []            | Action.PUT_MAPPING
    'new top-level field under a dynamic root'      | root([a: KEYWORD], [dynamic: 'true'])                       | root([a: KEYWORD, b: KEYWORD], [dynamic: 'true'])                                      | ['b']             | []                | []            | Action.PUT_MAPPING
    'property under typed properties'               | root([o: [properties: [a: KEYWORD]]], [dynamic: 'true'])    | root([o: [properties: [a: KEYWORD, b: KEYWORD]]], [dynamic: 'true'])                   | ['o.b']           | []                | []            | Action.PUT_MAPPING
    'typed property over dynamic: true'             | root([m: DYN])                                              | root([m: [type: 'object', dynamic: 'strict', properties: [a: KEYWORD]]])               | ['m.a']           | ['m.a']           | ['m:dynamic'] | Action.BACKFILL
    'typed property over dynamic: true, keys kept'  | root([m: DYN + [properties: [tier: KEYWORD]]])              | root([m: [type: 'object', dynamic: 'strict', properties: [tier: KEYWORD, a: KEYWORD]]]) | ['m.a']          | ['m.a']           | ['m:dynamic'] | Action.BACKFILL
    'typed over dynamic: true, only picked-up keys' | root([m: DYN + [properties: [tier: KEYWORD]]])              | root([m: [type: 'object', dynamic: 'strict', properties: [tier: KEYWORD]]])            | []                | []                | ['m:dynamic'] | Action.PUT_MAPPING
    'typed property over a dynamic nested field'    | root([n: NESTED_DYN])                                       | root([n: [type: 'nested', properties: [a: KEYWORD]]])                                  | ['n.a']           | ['n.a']           | ['n:dynamic'] | Action.BACKFILL
    'empty object under a dynamic root'             | root([o: [type: 'object']], [dynamic: 'true'])              | root([o: [type: 'object', properties: [a: KEYWORD]]], [dynamic: 'true'])               | ['o.a']           | ['o.a']           | []            | Action.BACKFILL
    'empty object, root without dynamic'            | [properties: [o: [type: 'object']]]                         | [properties: [o: [properties: [a: KEYWORD]]]]                                          | ['o.a']           | ['o.a']           | []            | Action.BACKFILL
    'empty object inheriting dynamic from a parent' | root([p: [properties: [o: [type: 'object']]]], [dynamic: true]) | root([p: [properties: [o: [properties: [a: KEYWORD]]]]], [dynamic: true])          | ['p.o.a']         | ['p.o.a']         | []            | Action.BACKFILL
    'empty object under a strict root'              | root([o: [type: 'object']])                                 | root([o: [type: 'object', properties: [a: KEYWORD]]])                                  | ['o.a']           | []                | []            | Action.PUT_MAPPING
    'empty object that turns dynamic off'           | root([o: [type: 'object', dynamic: 'false']], [dynamic: 'true']) | root([o: [type: 'object', dynamic: false, properties: [a: KEYWORD]]], [dynamic: 'true']) | ['o.a']     | []                | []            | Action.PUT_MAPPING
    'empty object under a parent that is strict'    | root([p: [dynamic: 'strict', properties: [o: [type: 'object']]]], [dynamic: 'true']) | root([p: [dynamic: 'strict', properties: [o: [properties: [a: KEYWORD]]]]], [dynamic: 'true']) | ['p.o.a'] | [] | [] | Action.PUT_MAPPING
  }

  @Unroll
  def "a change to #param #outcome"() {
    when:
    MappingDiff diff = DiffClassifier.diff(root([f: [type: 'keyword', (param): before]]),
        root([f: [type: 'keyword', (param): after]]), [:], [:])

    then:
    diff.paramChanged()*.path == ["f:${param}".toString()]
    DiffClassifier.classify(diff, RemovedFieldPolicy.KEEP) == action

    where:
    param             | before           | after            | action
    'analyzer'        | 'english'        | 'standard'       | Action.REINDEX
    'search_analyzer' | 'english'        | 'standard'       | Action.REINDEX
    'index'           | true             | false            | Action.REINDEX
    'doc_values'      | true             | false            | Action.REINDEX
    'format'          | 'epoch_millis'   | 'basic_date'     | Action.REINDEX
    'ignore_above'    | 256              | 10               | Action.REINDEX
    'normalizer'      | 'lowercase'      | 'folding'        | Action.REINDEX
    'copy_to'         | ['all']          | ['other']        | Action.REINDEX
    'null_value'      | 'NULL'           | 'NONE'           | Action.REINDEX
    'coerce'          | true             | false            | Action.REINDEX
    'enabled'         | true             | false            | Action.REINDEX
    'boost'           | 1                | 2                | Action.REINDEX
    'dynamic'         | 'strict'         | 'false'          | Action.PUT_MAPPING

    outcome = action == Action.REINDEX ? 'needs a reindex' : 'is applied in place'
  }

  def "only dynamic is updatable in place"() {
    expect:
    DiffClassifier.UPDATABLE_PARAMS == ['dynamic'] as Set
  }

  def "removed fields reindex under the REINDEX policy"() {
    given:
    MappingDiff diff = DiffClassifier.diff(root([a: KEYWORD, b: KEYWORD]), root([a: KEYWORD]), [:], [:])

    expect:
    DiffClassifier.classify(diff, RemovedFieldPolicy.REINDEX) == Action.REINDEX
    diff.removed() == [new Change('b', [type: 'keyword', ignore_above: '256'], null)]
  }

  def "children of a dynamic object never trigger a reindex under the REINDEX policy"() {
    given: 'a map field after a document with labels.tier was indexed'
    Map live = root([labels: [type: 'object', dynamic: 'true', properties: [tier: TEXT]]])
    MappingDiff diff = DiffClassifier.diff(live, root([labels: [type: 'object', dynamic: true]]), [:], [:])

    expect:
    !diff.hasFieldChanges()
    DiffClassifier.classify(diff, RemovedFieldPolicy.REINDEX) == Action.NO_OP
  }

  @Unroll
  def "unindexed additions are backfilled unless something else needs a reindex: #description"() {
    given:
    def diff = new MappingDiff([new MappingDiff.Change('t.keyword', null, KEYWORD)], removed, typeChanged,
        paramChanged, false, [new MappingDiff.Change('t.keyword', null, KEYWORD)])

    expect:
    DiffClassifier.classify(diff, policy) == action

    where:
    description                       | removed                                   | typeChanged                                        | paramChanged                                          | policy                    | action
    'alone'                           | []                                        | []                                                 | []                                                    | RemovedFieldPolicy.KEEP    | Action.BACKFILL
    'with a dynamic change'           | []                                        | []                                                 | [new MappingDiff.Change('<root>:dynamic', 'true', 'strict')] | RemovedFieldPolicy.KEEP | Action.BACKFILL
    'with a removed field kept'       | [new MappingDiff.Change('old', KEYWORD, null)] | []                                            | []                                                    | RemovedFieldPolicy.KEEP    | Action.BACKFILL
    'with a removed field reindexed'  | [new MappingDiff.Change('old', KEYWORD, null)] | []                                            | []                                                    | RemovedFieldPolicy.REINDEX | Action.REINDEX
    'with a type change'              | []                                        | [new MappingDiff.Change('u', 'text', 'keyword')]   | []                                                    | RemovedFieldPolicy.KEEP    | Action.REINDEX
    'with an analyzer change'         | []                                        | []                                                 | [new MappingDiff.Change('t:analyzer', 'english', 'standard')] | RemovedFieldPolicy.KEEP | Action.REINDEX
  }

  def "settings #description"() {
    expect:
    DiffClassifier.diff([:], [:], existing, desired).settingsChanged() == changed
    DiffClassifier.settingsWarnings(existing, desired) == warnings

    where:
    description                          | existing                                                         | desired                                              | changed | warnings
    'are compared as strings under index' | [index: [refresh_interval: '1s', number_of_shards: '1']]        | [refresh_interval: '1s', number_of_shards: 1]        | false   | []
    'ignore keys not desired'             | [index: [refresh_interval: '1s']]                                | [:]                                                  | false   | []
    'detect a differing value'            | [index: [refresh_interval: '1s']]                                | [refresh_interval: '5s']                             | true    | []
    'detect a missing value'              | [index: [:]]                                                     | ['index.refresh_interval': '5s']                     | true    | []
    'compare nested analysis'             | [index: [analysis: [analyzer: [a: [type: 'standard', max_token_length: '5']]]]] | [index: [analysis: [analyzer: [a: [type: 'standard', max_token_length: 5]]]]] | false | []
    'never treat shard counts as changes' | [index: [number_of_shards: '2', number_of_replicas: '1']]       | [number_of_shards: 1, number_of_replicas: 0]         | false   | ['number_of_replicas is 1 but the proto asks for 0; not applied', 'number_of_shards is 2 but the proto asks for 1; not applied']
    'accept null settings'                | null                                                             | null                                                 | false   | []
  }

  def "a settings-only difference is not an empty diff but classifies as a no-op"() {
    given:
    MappingDiff diff = DiffClassifier.diff(root([:]), root([:]), [:], [refresh_interval: '5s'])

    expect:
    !diff.isEmpty()
    !diff.hasFieldChanges()
    !diff.additiveOnly()
    DiffClassifier.classify(diff, RemovedFieldPolicy.KEEP) == Action.NO_OP
    MappingDiff.empty().isEmpty()
  }

  def "normalize and param are exposed for tooling"() {
    expect:
    DiffClassifier.normalize([_meta: [:], dynamic: false, properties: [o: [properties: [:]]]]) ==
        [dynamic: 'false', properties: [o: [type: 'object', properties: [:]]]]
    DiffClassifier.param('a.b:analyzer') == 'analyzer'
    DiffClassifier.param('plain') == 'plain'
  }
}
