package com.phatjam98.elasticprotos.micronaut

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.phatjam98.elasticprotos.engine.EngineException
import com.phatjam98.elasticprotos.engine.SearchEngine
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.migration.Action
import com.phatjam98.elasticprotos.migration.IndexMigrator
import com.phatjam98.elasticprotos.migration.IndexPlan
import com.phatjam98.elasticprotos.migration.MappingDiff
import com.phatjam98.elasticprotos.migration.MigrationResult
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.testkit.InMemorySearchEngine
import io.micronaut.context.ApplicationContext
import io.micronaut.context.BeanContext
import org.slf4j.LoggerFactory
import spock.lang.Specification

class BootstrapSpec extends Specification {

  static final String CLASSES = "${Firm.name}, ${Document.name}"

  static ApplicationContext start(Map props) {
    ApplicationContext.run(['test.engine': 'memory'] + props)
  }

  static Set<String> aliases(InMemorySearchEngine engine) {
    engine.indices().values().flatten() as Set
  }

  def "creates the configured indices at startup"() {
    given:
    def ctx = start(['elastic-protos.bootstrap.classes': CLASSES])
    def engine = ctx.getBean(InMemorySearchEngine)

    expect:
    aliases(engine) == ['firms', 'documents'] as Set
    engine.indices().keySet().every { it.startsWith('firms-v') || it.startsWith('documents-v') }

    when: 'running it again is a no-op'
    def result = ctx.getBean(ElasticProtosBootstrap).run()

    then:
    result.ok()
    result.outcomes()*.plan()*.action() == [Action.NO_OP, Action.NO_OP]

    cleanup:
    ctx?.close()
  }

  def "accepts the class list as a YAML-style list"() {
    given:
    def ctx = start(['elastic-protos.bootstrap.classes': [Firm.name]])

    expect:
    aliases(ctx.getBean(InMemorySearchEngine)) == ['firms'] as Set

    cleanup:
    ctx?.close()
  }

  def "an unknown class aborts startup with a clear message"() {
    when:
    start(['elastic-protos.bootstrap.classes': 'com.example.Missing'])

    then:
    def e = thrown(ElasticProtosBootstrap.BootstrapException)
    e.message == 'elastic-protos.bootstrap.classes: cannot load class com.example.Missing'
    e.cause instanceof ClassNotFoundException
  }

  def "a class that is not a message aborts startup"() {
    when:
    start(['elastic-protos.bootstrap.classes': 'java.lang.String'])

    then:
    def e = thrown(RuntimeException)
    rootMessages(e).any { it.contains('java.lang.String is not a generated protobuf message class') }
  }

  def "a blocked plan aborts startup by default and names the problem"() {
    when:
    start(['elastic-protos.bootstrap.classes': Firm.name, 'test.preexisting-index': 'firms'])

    then:
    def e = thrown(RuntimeException)
    rootMessages(e).any { it.contains('elastic-protos bootstrap failed: firms BLOCKED') }
  }

  def "a blocked plan lets startup continue when fail-on-error is false"() {
    given:
    def ctx = start([
        'elastic-protos.bootstrap.classes'      : CLASSES,
        'elastic-protos.bootstrap.fail-on-error': false,
        'elastic-protos.bootstrap.failure-policy': 'CONTINUE',
        'test.preexisting-index'                : 'firms',
    ])
    def engine = ctx.getBean(InMemorySearchEngine)

    expect:
    ctx.running
    engine.indices().containsKey('firms')
    aliases(engine) == ['documents'] as Set

    when:
    MigrationResult result = ctx.getBean(ElasticProtosBootstrap).run()

    then:
    !result.ok()
    result.problems()*.plan()*.alias() == ['firms']

    cleanup:
    ctx?.close()
  }

  /**
   * A bootstrap whose planning throws, as it does when the cluster is unreachable. IndexMigrator is
   * final, so the migrator is real and the engine underneath it fails every call.
   */
  ElasticProtosBootstrap unreachableBootstrap(boolean failOnError) {
    SearchEngine engine = Stub(SearchEngine) {
      _ >> { throw new EngineException(EngineException.Kind.IO, 0, 'connection refused') }
    }
    def migrator = IndexMigrator.builder().engine(engine).build()
    def config = new ElasticProtosConfiguration()
    config.bootstrap.classes = [Firm.name]
    config.bootstrap.failOnError = failOnError
    BeanContext beanContext = Stub(BeanContext) {
      getClassLoader() >> BootstrapSpec.classLoader
    }
    new ElasticProtosBootstrap(migrator, config, beanContext)
  }

  def "a planning failure aborts startup by default"() {
    when:
    unreachableBootstrap(true).run()

    then:
    def e = thrown(ElasticProtosBootstrap.BootstrapException)
    e.message == 'elastic-protos bootstrap failed: connection refused'
    e.cause instanceof EngineException
  }

  def "a planning failure lets startup continue when fail-on-error is false"() {
    when:
    def result = unreachableBootstrap(false).run()

    then:
    noExceptionThrown()
    result == null
  }

  def "a dry run creates nothing"() {
    given:
    def ctx = start(['elastic-protos.bootstrap.classes': CLASSES, 'elastic-protos.bootstrap.dry-run': true])
    def engine = ctx.getBean(InMemorySearchEngine)

    expect:
    engine.indices().isEmpty()
    ctx.getBean(ElasticProtosBootstrap).run().outcomes()*.status() ==
        [MigrationResult.Status.DRY_RUN, MigrationResult.Status.DRY_RUN]

    cleanup:
    ctx?.close()
  }

  def "no classes means no bootstrap"() {
    given:
    def ctx = start([:])

    expect:
    ctx.getBean(InMemorySearchEngine).indices().isEmpty()
    ctx.getBean(ElasticProtosBootstrap).run() == null

    cleanup:
    ctx?.close()
  }

  def "the bootstrap bean is absent when disabled"() {
    given:
    def ctx = start(['elastic-protos.bootstrap.enabled': false, 'elastic-protos.bootstrap.classes': CLASSES])

    expect:
    !ctx.containsBean(ElasticProtosBootstrap)
    ctx.getBean(InMemorySearchEngine).indices().isEmpty()

    cleanup:
    ctx?.close()
  }

  ListAppender<ILoggingEvent> capture() {
    Logger logger = (Logger) LoggerFactory.getLogger(ElasticProtosBootstrap)
    ListAppender<ILoggingEvent> appender = new ListAppender<>()
    appender.start()
    logger.addAppender(appender)
    appender
  }

  void release(ListAppender<ILoggingEvent> appender) {
    ((Logger) LoggerFactory.getLogger(ElasticProtosBootstrap)).detachAppender(appender)
  }

  static ILoggingEvent finished(ListAppender<ILoggingEvent> appender) {
    appender.list.find { it.formattedMessage.startsWith('elastic-protos bootstrap finished') }
  }

  static MigrationResult.Outcome outcome(Class type, Action action, MigrationResult.Status status,
      List<String> warnings) {
    def plan = new IndexPlan(ProtoMapper.withDefaults().map(type), action, null, null,
        MappingDiff.empty(), 'r', [])
    new MigrationResult.Outcome(plan, status, 'x', [], null, 1L, 1L, warnings)
  }

  def "a clean run logs the result at INFO"() {
    given:
    def ctx = start(['elastic-protos.bootstrap.classes': CLASSES])
    def appender = capture()

    when:
    ctx.getBean(ElasticProtosBootstrap).run()

    then:
    finished(appender).level == Level.INFO
    finished(appender).formattedMessage == 'elastic-protos bootstrap finished: 2 index(es) ok'

    cleanup:
    release(appender)
    ctx?.close()
  }

  def "outcome warnings turn the success line into a WARN that names them"() {
    given:
    def appender = capture()
    def result = new MigrationResult([
        outcome(Firm, Action.NO_OP, MigrationResult.Status.SKIPPED, []),
        outcome(Document, Action.REINDEX, MigrationResult.Status.APPLIED,
            ['the migration script left 9 documents in b from 10 in a', 'second']),
    ])

    when:
    ElasticProtosBootstrap.logSuccess(result)

    then:
    result.ok()
    finished(appender).level == Level.WARN
    finished(appender).formattedMessage == 'elastic-protos bootstrap finished: 2 index(es) ok,' +
        ' with warnings: documents: the migration script left 9 documents in b from 10 in a; second'

    cleanup:
    release(appender)
  }

  def "the problem description includes outcome warnings"() {
    given:
    def result = new MigrationResult([
        outcome(Firm, Action.BLOCKED, MigrationResult.Status.BLOCKED, []),
        outcome(Document, Action.REINDEX, MigrationResult.Status.APPLIED, ['count changed']),
    ])

    expect:
    ElasticProtosBootstrap.describe(result) ==
        'firms BLOCKED/BLOCKED: r; warnings: documents: count changed'
  }

  static List<String> rootMessages(Throwable e) {
    List<String> messages = []
    for (Throwable t = e; t != null; t = t.cause) {
      messages << String.valueOf(t.message)
    }
    messages
  }
}
