package com.phatjam98.elasticprotos.engine

import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer
import java.util.function.Consumer
import spock.lang.Specification

class BulkBufferSpec extends Specification {

  SearchEngine engine = Mock()

  static BulkResult ok(List<BulkOp> ops) {
    new BulkResult(false, ops.collect { new BulkResult.Item('index', it.index(), it.id(), 201, null) }, 1)
  }

  def "flushes when the batch reaches maxActions"() {
    given:
    List<BulkResult> results = []
    def buffer = BulkBuffer.builder().engine(engine).maxActions(2).listener { results << it }.build()

    when:
    buffer.add(BulkOp.index('a', '1', [n: 1]))

    then:
    0 * engine.bulk(_)
    buffer.pending() == 1

    when:
    buffer.add(BulkOp.index('a', '2', [n: 2]))

    then:
    1 * engine.bulk({ it*.id() == ['1', '2'] }) >> { args -> ok(args[0]) }
    buffer.pending() == 0
    results.size() == 1
    results[0].items().size() == 2

    cleanup:
    buffer.close()
  }

  def "flushes when the batch reaches maxBytes"() {
    given:
    def buffer = BulkBuffer.builder().engine(engine).maxBytes(BulkBuffer.ACTION_OVERHEAD + 10).build()

    when:
    buffer.add(BulkOp.index('a', '1', [text: 'a long enough value']))

    then:
    1 * engine.bulk(_) >> { args -> ok(args[0]) }

    cleanup:
    buffer.close()
  }

  def "estimates sizes from the canonical JSON and handles deletes"() {
    expect:
    BulkBuffer.estimate(BulkOp.delete('ab', '123')) == BulkBuffer.ACTION_OVERHEAD + 5
    BulkBuffer.estimate(BulkOp.index('ab', null, [k: 'v'])) == BulkBuffer.ACTION_OVERHEAD + 2 + 9
  }

  def "flushes on the interval from a background thread"() {
    given:
    def latch = new CountDownLatch(1)
    def threads = new CopyOnWriteArrayList<String>()
    engine.bulk(_) >> { args -> threads << Thread.currentThread().name; ok(args[0]) }
    def buffer = BulkBuffer.builder()
        .engine(engine)
        .flushInterval(Duration.ofMillis(20))
        .listener { latch.countDown() }
        .build()

    when:
    buffer.add(BulkOp.index('a', '1', [n: 1]))

    then:
    latch.await(5, TimeUnit.SECONDS)
    threads == ['elastic-protos-bulk-flush']
    buffer.pending() == 0

    cleanup:
    buffer.close()
  }

  def "close flushes what is waiting and rejects later adds"() {
    given:
    def buffer = BulkBuffer.builder().engine(engine).flushInterval(Duration.ofMinutes(5)).build()
    buffer.add(BulkOp.index('a', '1', [n: 1]))
    buffer.add(BulkOp.delete('a', '2'))

    when:
    buffer.close()

    then:
    1 * engine.bulk({ it.size() == 2 }) >> { args -> ok(args[0]) }

    when:
    buffer.close()
    buffer.add(BulkOp.index('a', '3', [n: 3]))

    then:
    0 * engine.bulk(_)
    thrown(IllegalStateException)
  }

  def "flush is a no-op when nothing is waiting"() {
    given:
    def buffer = BulkBuffer.builder().engine(engine).build()

    expect:
    buffer.flush() == null

    when:
    buffer.close()

    then:
    0 * engine.bulk(_)
  }

  def "flush returns the result synchronously"() {
    given:
    def buffer = BulkBuffer.builder().engine(engine).build()
    buffer.add(BulkOp.create('a', '1', [n: 1]))

    when:
    def result = buffer.flush()

    then:
    1 * engine.bulk(_) >> { args -> ok(args[0]) }
    result.items()*.id() == ['1']

    cleanup:
    buffer.close()
  }

  def "engine failures hand the failed batch to the error listener"() {
    given:
    List<List<BulkOp>> batches = []
    List<Throwable> errors = []
    List<BulkResult> results = []
    def buffer = BulkBuffer.builder().engine(engine)
        .listener { results << it }
        .errorListener({ batch, error -> batches << batch; errors << error } as BiConsumer)
        .build()
    def first = BulkOp.update('a', '1', [n: 1])
    def second = BulkOp.delete('a', '2')
    buffer.add(first)
    buffer.add(second)

    when:
    def result = buffer.flush()

    then:
    1 * engine.bulk(_) >> { throw new EngineException(EngineException.Kind.TRANSIENT, 503, 'down') }
    result == null
    batches == [[first, second]]
    errors.size() == 1
    errors[0].message.contains('down')
    results.isEmpty()
    buffer.pending() == 0

    when: 'the handed-back batch is immutable'
    batches[0].add(first)

    then:
    thrown(UnsupportedOperationException)

    cleanup:
    buffer.close()
  }

  def "a failed batch is not retried silently"() {
    given:
    List<List<BulkOp>> failed = []
    def buffer = BulkBuffer.builder().engine(engine)
        .errorListener({ batch, error -> failed << batch } as BiConsumer)
        .build()
    def lost = BulkOp.index('a', '1', [n: 1])
    def next = BulkOp.index('a', '2', [n: 2])

    when:
    buffer.add(lost)
    buffer.flush()
    buffer.add(next)
    buffer.flush()
    buffer.close()

    then:
    1 * engine.bulk([lost]) >> { throw new IllegalStateException('boom') }
    1 * engine.bulk([next]) >> { args -> ok(args[0]) }
    0 * engine.bulk(_)
    failed == [[lost]]
  }

  def "the deprecated error listener still receives the exception"() {
    given:
    List<Throwable> errors = []
    def buffer = BulkBuffer.builder().engine(engine)
        .errorListener({ errors << it } as Consumer)
        .build()
    buffer.add(BulkOp.index('a', '1', [n: 1]))

    when:
    buffer.flush()

    then:
    1 * engine.bulk(_) >> { throw new IllegalStateException('boom') }
    errors*.message == ['boom']

    cleanup:
    buffer.close()
  }

  def "close notifies the listener and then throws when the final flush fails"() {
    given:
    List<List<BulkOp>> failed = []
    def buffer = BulkBuffer.builder().engine(engine)
        .errorListener({ batch, error -> failed << batch } as BiConsumer)
        .build()
    def op = BulkOp.index('a', '1', [n: 1])
    buffer.add(op)

    when:
    buffer.close()

    then:
    1 * engine.bulk(_) >> { throw failure }
    def e = thrown(EngineException)
    failed == [[op]]
    expected(e, failure)

    when: 'a second close has nothing left to send'
    buffer.close()

    then:
    0 * engine.bulk(_)
    noExceptionThrown()

    where:
    failure                                                         | expected
    new EngineException(EngineException.Kind.TRANSIENT, 429, 'busy') | { thrownE, f -> thrownE.is(f) }
    new IllegalStateException('boom')                               | { thrownE, f -> thrownE.cause.is(f) && thrownE.kind == EngineException.Kind.OTHER && thrownE.message.contains('1 operations failed: boom') }
  }

  def "a throwing error listener does not replace the close() failure"() {
    given:
    def failure = new EngineException(EngineException.Kind.TRANSIENT, 429, 'busy')
    def listenerFailure = new IllegalStateException('listener')
    def buffer = BulkBuffer.builder().engine(engine)
        .errorListener({ batch, error -> throw listenerFailure } as BiConsumer)
        .build()
    buffer.add(BulkOp.index('a', '1', [n: 1]))

    when:
    buffer.close()

    then:
    1 * engine.bulk(_) >> { throw failure }
    def e = thrown(EngineException)
    e.is(failure)
    e.suppressed.toList() == [listenerFailure]
  }

  def "close with a successful final flush does not throw"() {
    given:
    def buffer = BulkBuffer.builder().engine(engine).build()
    buffer.add(BulkOp.index('a', '1', [n: 1]))

    when:
    buffer.close()

    then:
    1 * engine.bulk(_) >> { args -> ok(args[0]) }
    noExceptionThrown()
  }

  def "default listeners log instead of throwing"() {
    given:
    def buffer = BulkBuffer.builder().engine(engine).build()
    buffer.add(BulkOp.index('a', '1', [n: 1]))
    buffer.add(BulkOp.index('a', '2', [n: 2]))

    when:
    buffer.flush()
    buffer.add(BulkOp.index('a', '3', [n: 3]))
    buffer.flush()

    then:
    1 * engine.bulk({ it.size() == 2 }) >> new BulkResult(true, [new BulkResult.Item('index', 'a', '1', 400, 'bad')], 1)
    1 * engine.bulk({ it.size() == 1 }) >> { throw new IllegalStateException('boom') }
    noExceptionThrown()

    cleanup:
    buffer.close()
  }

  def "a throwing listener does not stop the timer"() {
    given:
    def latch = new CountDownLatch(2)
    engine.bulk(_) >> { args -> ok(args[0]) }
    def buffer = BulkBuffer.builder().engine(engine).flushInterval(Duration.ofMillis(10))
        .listener { latch.countDown(); throw new IllegalStateException('listener') }
        .build()

    when:
    buffer.add(BulkOp.index('a', '1', [n: 1]))
    Thread.sleep(50)
    buffer.add(BulkOp.index('a', '2', [n: 2]))

    then:
    latch.await(5, TimeUnit.SECONDS)

    cleanup:
    buffer.close()
  }

  def "the builder validates its arguments"() {
    when:
    BulkBuffer.builder().maxActions(0)

    then:
    thrown(IllegalArgumentException)

    when:
    BulkBuffer.builder().maxBytes(0)

    then:
    thrown(IllegalArgumentException)

    when:
    BulkBuffer.builder().build()

    then:
    thrown(NullPointerException)

    when:
    BulkBuffer.builder().engine(engine).listener(null)

    then:
    thrown(NullPointerException)

    when:
    BulkBuffer.builder().engine(engine).errorListener((BiConsumer) null)

    then:
    thrown(NullPointerException)

    when:
    BulkBuffer.builder().engine(engine).errorListener((Consumer) null)

    then:
    thrown(NullPointerException)
  }
}
