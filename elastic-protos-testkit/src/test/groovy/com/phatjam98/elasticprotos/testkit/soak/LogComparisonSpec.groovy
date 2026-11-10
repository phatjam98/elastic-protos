package com.phatjam98.elasticprotos.testkit.soak

import com.phatjam98.elasticprotos.testkit.soak.LogComparison.Anomaly
import com.phatjam98.elasticprotos.testkit.soak.LogComparison.Timeline
import com.phatjam98.elasticprotos.testkit.soak.LogComparison.Violation
import com.phatjam98.elasticprotos.testkit.soak.OpLog.Entry
import com.phatjam98.elasticprotos.testkit.soak.OpLog.OpType
import com.phatjam98.elasticprotos.testkit.soak.WriteFailures.Failure
import com.phatjam98.elasticprotos.testkit.soak.WriteFailures.Mode
import spock.lang.Specification

class LogComparisonSpec extends Specification {

  // copy starts after 1000, the catch-up after 2000, the alias moves at 3000
  static final Timeline ZD = new Timeline('old', 1000, 2000, 3000, true)
  static final Timeline STRICT = new Timeline('old', Long.MAX_VALUE, Long.MAX_VALUE, 3000, false)

  static Entry seed(Map doc) { new Entry(OpType.SEED, doc, 0, 1, 'old', false) }

  static Entry onOld(OpType type, Map body, long at, boolean found = false) {
    new Entry(type, body, at, at + 1, 'old', found)
  }

  static Entry onNew(OpType type, Map body, long at, boolean found = false) {
    new Entry(type, body, at, at + 1, 'new', found)
  }

  static LogComparison.Result compare(Map<String, List<Entry>> log, Map<String, Map> actual,
      Timeline timeline = ZD, Set<String> uncertain = [] as Set) {
    LogComparison.compare(log, uncertain, actual, timeline, 10)
  }

  def 'replay applies full writes, partial updates and deletes in order'() {
    expect:
    OpLog.replay([seed([a: 1, b: 1]), onOld(OpType.UPDATE, [b: 2], 10)]) == [a: 1, b: 2]
    OpLog.replay([seed([a: 1]), onOld(OpType.DELETE, null, 10)]) == null
    OpLog.replay([onOld(OpType.UPDATE, [b: 2], 10)]) == null
    OpLog.replay([seed([a: 1]), onOld(OpType.INDEX, [c: 3], 10)]) == [c: 3]
  }

  def 'documents the log explains match, numbers compare by value'() {
    given:
    def log = [x: [seed([id: 'x', n: 1]), onNew(OpType.UPDATE, [n: 2L], 3100)],
               y: [seed([id: 'y']), onOld(OpType.DELETE, null, 500, true)]]

    when:
    def result = compare(log, [x: [id: 'x', n: 2]])

    then:
    result.checked() == 2
    result.matched() == 2
    result.violationCount() == 0
    result.anomalyCount() == 0
  }

  def 'missing, extra, unknown and differing documents are violations'() {
    given:
    def log = [m: [seed([id: 'm'])],
               e: [seed([id: 'e']), onNew(OpType.DELETE, null, 3500, true)],
               d: [seed([id: 'd', n: 1]), onNew(OpType.INDEX, [id: 'd', n: 2], 3500, true)]]

    when:
    def result = compare(log, [e: [id: 'e'], d: [id: 'd', n: 1], u: [id: 'u']])

    then:
    result.violations() == [(Violation.MISSING): 1L, (Violation.EXTRA): 2L,
                            (Violation.DIFFERING): 1L]
    result.findings()*.id().toSet() == ['m', 'e', 'd', 'u'] as Set
    result.differing() == ['d', 'e', 'm', 'u'] as Set
  }

  def 'uncertain ids are skipped'() {
    when:
    def result = compare([x: [seed([id: 'x'])]], [:], ZD, ['x'] as Set)

    then:
    result.checked() == 0
    result.uncertain() == 1
    result.violationCount() == 0
  }

  def 'a partial update after the swap on a copy that missed late old-index writes is documented'() {
    given: 'the old index took a full write after the catch-up began; the update hit the stale copy'
    def entries = [seed([id: 'x', t: 'seed', n: 0]),
                   onOld(OpType.INDEX, [id: 'x', t: 'late', n: 1], 2500, true),
                   onNew(OpType.UPDATE, [n: 2], 3100)]

    expect:
    compare([x: entries], [x: [id: 'x', t: 'seed', n: 2]]).anomalies()[Anomaly.STALE_PARTIAL_UPDATE] == 1
  }

  def 'a lost write the catch-up should have copied is a violation, not a stale update'() {
    given: 'the full write was sent before the catch-up began'
    def entries = [seed([id: 'x', t: 'seed', n: 0]),
                   onOld(OpType.INDEX, [id: 'x', t: 'early', n: 1], 1500, true),
                   onNew(OpType.UPDATE, [n: 2], 3100)]

    when:
    def result = compare([x: entries], [x: [id: 'x', t: 'seed', n: 2]])

    then:
    result.violations()[Violation.DIFFERING] == 1
    result.anomalyCount() == 0
  }

  def 'a stale update on a ghost of a document deleted during the copy is told apart'() {
    given: 'deleted during the copy, created again after the catch-up began, updated after the swap'
    def entries = [seed([id: 'x', t: 'seed', n: 0]),
                   onOld(OpType.DELETE, null, 1500, true),
                   onOld(OpType.CREATE, [id: 'x', t: 'again', n: 1], 2500),
                   onNew(OpType.UPDATE, [n: 2], 3100)]

    expect:
    compare([x: entries], [x: [id: 'x', t: 'seed', n: 2]])
        .anomalies()[Anomaly.STALE_PARTIAL_UPDATE_ON_GHOST] == 1
  }

  def 'a ghost from before the copy started is a violation'() {
    given: 'deleted before the copy began, so no copy can hold it'
    def entries = [seed([id: 'x', t: 'seed', n: 0]),
                   onOld(OpType.DELETE, null, 500, true),
                   onOld(OpType.CREATE, [id: 'x', t: 'again', n: 1], 2500),
                   onNew(OpType.UPDATE, [n: 2], 3100)]

    expect:
    compare([x: entries], [x: [id: 'x', t: 'seed', n: 2]]).violations()[Violation.DIFFERING] == 1
  }

  def 'a ghost needs the document deleted when the catch-up read it'() {
    given: 'deleted and created again during the copy: the catch-up copied the new version'
    def entries = [seed([id: 'x', t: 'seed', n: 0]),
                   onOld(OpType.DELETE, null, 1500, true),
                   onOld(OpType.CREATE, [id: 'x', t: 'again', n: 1], 1600),
                   onOld(OpType.INDEX, [id: 'x', t: 'late', n: 3], 2500, true),
                   onNew(OpType.UPDATE, [n: 2], 3100)]

    expect:
    compare([x: entries], [x: [id: 'x', t: 'seed', n: 2]]).violations()[Violation.DIFFERING] == 1
  }

  def 'a late create deleted through the alias before the reconcile and brought back is documented'() {
    given:
    def created = [id: 'x', t: 'late']
    def entries = [onOld(OpType.CREATE, created, 2500), onNew(OpType.DELETE, null, 3100, false)]

    expect:
    compare([x: entries], [x: created]).anomalies()[Anomaly.RESURRECTED_AFTER_DELETE] == 1
  }

  def 'the same with writes through the alias before the delete is documented too'() {
    given: 'the new index never held it at the swap: the first write after it created it there'
    def created = [id: 'x', t: 'late']
    def entries = [seed([id: 'x', t: 'seed']),
                   onOld(OpType.DELETE, null, 500, true),
                   onOld(OpType.CREATE, created, 2500),
                   onNew(OpType.INDEX, [id: 'x', t: 'new'], 3100, false),
                   onNew(OpType.UPDATE, [n: 1], 3200),
                   onNew(OpType.DELETE, null, 3300, true)]

    expect:
    compare([x: entries], [x: created]).anomalies()[Anomaly.RESURRECTED_AFTER_DELETE] == 1
  }

  def 'a resurrection is a violation when #why'() {
    expect:
    compare([x: entries], [x: [id: 'x', t: 'late']]).violations()[Violation.EXTRA] == 1

    where:
    why                                            | entries
    'the create was sent before the catch-up'      | [onOld(OpType.CREATE, [id: 'x', t: 'late'], 1500),
                                                      onNew(OpType.DELETE, null, 3100, false)]
    'the new index held the document at the swap'  | [onOld(OpType.CREATE, [id: 'x', t: 'late'], 2500),
                                                      onNew(OpType.DELETE, null, 3100, true)]
    'the first write after the swap found it'      | [onOld(OpType.CREATE, [id: 'x', t: 'late'], 2500),
                                                      onNew(OpType.INDEX, [id: 'x', t: 'n'], 3100, true),
                                                      onNew(OpType.DELETE, null, 3200, true)]
  }

  def 'without a zero-downtime timeline nothing is documented'() {
    given:
    def entries = [seed([id: 'x', t: 'seed', n: 0]),
                   onOld(OpType.INDEX, [id: 'x', t: 'late', n: 1], 2500, true),
                   onNew(OpType.UPDATE, [n: 2], 3100)]

    expect:
    compare([x: entries], [x: [id: 'x', t: 'seed', n: 2]], STRICT).violations()[Violation.DIFFERING] == 1
  }

  def 'write failures are documented by mode, phase and history'() {
    given:
    def late = [onOld(OpType.CREATE, [id: 'x'], 2500)]
    def early = [onOld(OpType.CREATE, [id: 'x'], 1500)]

    expect:
    WriteFailures.documented(failure('BLOCKED', 'INDEX', 3500), Mode.ZERO_DOWNTIME, ZD, []) == 'BLOCKED_AFTER_SWAP'
    WriteFailures.documented(failure('BLOCKED', 'INDEX', 2900), Mode.ZERO_DOWNTIME, ZD, []) == null
    WriteFailures.documented(failure('NOT_FOUND', 'UPDATE', 3500), Mode.ZERO_DOWNTIME, ZD, late) ==
        'UPDATE_404_BEFORE_RECONCILE'
    WriteFailures.documented(failure('NOT_FOUND', 'UPDATE', 3500), Mode.ZERO_DOWNTIME, ZD, early) == null
    WriteFailures.documented(failure('NOT_FOUND', 'UPDATE', 2900), Mode.ZERO_DOWNTIME, ZD, late) == null
    WriteFailures.documented(failure('CONFLICT', 'UPDATE', 3500), Mode.ZERO_DOWNTIME, ZD, late) == null
    WriteFailures.documented(failure('BLOCKED', 'DELETE', 1500), Mode.BLOCKING, STRICT, []) ==
        'BLOCKING_WRITE_BLOCK'
    WriteFailures.documented(failure('NOT_FOUND', 'UPDATE', 1500), Mode.BLOCKING, STRICT, []) == null
    WriteFailures.documented(failure('CONFLICT', 'UPDATE', 1500), Mode.BACKFILL, STRICT, []) == null
  }

  def 'the summary counts failures by phase, operation and kind and lists violations'() {
    given:
    def failures = new WriteFailures()
    failures.add(failure('BLOCKED', 'INDEX', 3500))
    failures.add(failure('BLOCKED', 'INDEX', 2900))

    when:
    def summary = failures.summarize(Mode.ZERO_DOWNTIME, ZD, [:], 5)

    then:
    summary.attemptsFailed == 2L
    summary.byPhaseOpKind == ['P/INDEX/BLOCKED': 2L]
    summary.byClass == [BLOCKED_AFTER_SWAP: 1L, VIOLATION: 1L]
    summary.violations == 1L
    summary.violationSamples.size() == 1
  }

  def 'the log tracks in-flight writes for readers'() {
    given:
    def log = new OpLog()
    log.ack('x', seed([id: 'x']))

    when:
    def view = log.view('x')

    then:
    view.expected() == [id: 'x']
    !log.changedSince('x', view)

    when:
    log.begin('x')

    then:
    log.view('x') == null
    log.changedSince('x', view)

    when:
    log.unknown('x')

    then:
    log.view('x') == null
    log.uncertainIds() == ['x'] as Set
  }

  static Failure failure(String kind, String op, long at) {
    new Failure('x', op, kind, 0, 'm', at - 5, at, 'P', false)
  }
}
