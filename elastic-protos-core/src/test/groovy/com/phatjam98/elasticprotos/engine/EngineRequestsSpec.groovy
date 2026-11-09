package com.phatjam98.elasticprotos.engine

import spock.lang.Specification
import spock.lang.Unroll

/** Write options and bulk operations: compatibility constructors and conditional writes. */
class EngineRequestsSpec extends Specification {

  def "the two-argument index options are unconditional"() {
    expect:
    !IndexOptions.defaults().conditional()
    IndexOptions.defaults().ifSeqNo() == null
    IndexOptions.defaults().ifPrimaryTerm() == null
    IndexOptions.refreshed() == new IndexOptions(false, IndexOptions.Refresh.IMMEDIATE, null, null)
  }

  def "ifMatch sets the sequence number and primary term, and the with methods keep them"() {
    when:
    def options = IndexOptions.ifMatch(4, 1)

    then:
    options.conditional()
    options.ifSeqNo() == 4L
    options.ifPrimaryTerm() == 1L
    !options.createOnly()
    options.refresh() == IndexOptions.Refresh.NONE
    options.withRefresh(IndexOptions.Refresh.WAIT_FOR) ==
        new IndexOptions(false, IndexOptions.Refresh.WAIT_FOR, 4L, 1L)
    options.withCreateOnly(false) == options
    IndexOptions.defaults().withCreateOnly(true) == new IndexOptions(true, IndexOptions.Refresh.NONE)
  }

  @Unroll
  def "index options and bulk operations need both or neither of seqNo #seqNo and term #term"() {
    when:
    new IndexOptions(false, IndexOptions.Refresh.NONE, seqNo, term)

    then:
    def e = thrown(IllegalArgumentException)
    e.message == "ifSeqNo and ifPrimaryTerm must be set together: ${seqNo}, ${term}"

    when:
    new BulkOp(BulkOp.Type.INDEX, 'i', '1', [:], seqNo, term)

    then:
    thrown(IllegalArgumentException)

    where:
    seqNo | term
    1L    | null
    null  | 1L
  }

  @Unroll
  def "index options reject #name"() {
    when:
    new IndexOptions(createOnly, IndexOptions.Refresh.NONE, seqNo, term)

    then:
    def e = thrown(IllegalArgumentException)
    e.message == message

    where:
    name                          | createOnly | seqNo | term || message
    'a negative sequence number'  | false      | -1L   | 1L   || 'ifSeqNo must not be negative: -1'
    'a primary term of 0'         | false      | 0L    | 0L   || 'ifPrimaryTerm must be at least 1: 0'
    'a conditional create'        | true       | 0L    | 1L   || 'create operations do not support compare and set: use ifSeqNo/ifPrimaryTerm or createOnly, not both'
  }

  def "a conditional write with createOnly cannot be made through withCreateOnly either"() {
    when:
    IndexOptions.ifMatch(1, 1).withCreateOnly(true)

    then:
    thrown(IllegalArgumentException)
  }

  @Unroll
  def "bulk operations reject #name"() {
    when:
    new BulkOp(type, 'i', '1', [:], seqNo, term)

    then:
    def e = thrown(IllegalArgumentException)
    e.message == message

    where:
    name                         | type             | seqNo | term || message
    'a negative sequence number' | BulkOp.Type.INDEX  | -5L | 1L   || 'ifSeqNo must not be negative: -5'
    'a primary term of 0'        | BulkOp.Type.DELETE | 1L  | 0L   || 'ifPrimaryTerm must be at least 1: 0'
    'a conditional create'       | BulkOp.Type.CREATE | 1L  | 1L   || 'create operations do not support compare and set: use INDEX with ifSeqNo'
  }

  def "a sequence number of 0 and a primary term of 1 are the smallest valid values"() {
    expect:
    IndexOptions.ifMatch(0, 1).conditional()
    BulkOp.deleteIfMatch('i', '1', 0, 1).ifSeqNo() == 0L
  }

  def "bulk factories are unconditional, the IfMatch ones are conditional"() {
    expect:
    BulkOp.index('i', '1', [a: 1]) == new BulkOp(BulkOp.Type.INDEX, 'i', '1', [a: 1], null, null)
    BulkOp.create('i', '1', [a: 1]).ifSeqNo() == null
    BulkOp.update('i', '1', [a: 1]).ifPrimaryTerm() == null
    BulkOp.delete('i', '1') == new BulkOp(BulkOp.Type.DELETE, 'i', '1', null)
    BulkOp.indexIfMatch('i', '1', [a: 1], 3, 2) == new BulkOp(BulkOp.Type.INDEX, 'i', '1', [a: 1], 3L, 2L)
    BulkOp.deleteIfMatch('i', '1', 3, 2) == new BulkOp(BulkOp.Type.DELETE, 'i', '1', null, 3L, 2L)
  }
}
