package com.phatjam98.elasticprotos.engine

import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
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

  def "engine failures go to the error listener and drop the batch"() {
    given:
    List<Throwable> errors = []
    List<BulkResult> results = []
    def buffer = BulkBuffer.builder().engine(engine)
        .listener { results << it }
        .errorListener { errors << it }
        .build()
    buffer.add(BulkOp.update('a', '1', [n: 1]))

    when:
    def result = buffer.flush()

    then:
    1 * engine.bulk(_) >> { throw new EngineException(EngineException.Kind.TRANSIENT, 503, 'down') }
    result == null
    errors.size() == 1
    errors[0].message.contains('down')
    results.isEmpty()
    buffer.pending() == 0

    cleanup:
    buffer.close()
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
  }
}
