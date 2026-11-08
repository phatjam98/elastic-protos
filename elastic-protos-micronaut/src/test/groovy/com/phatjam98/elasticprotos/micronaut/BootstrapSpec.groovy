package com.phatjam98.elasticprotos.micronaut

import com.phatjam98.elasticprotos.migration.Action
import com.phatjam98.elasticprotos.migration.MigrationResult
import com.phatjam98.elasticprotos.proto.example.Document
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.testkit.InMemorySearchEngine
import io.micronaut.context.ApplicationContext
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

  static List<String> rootMessages(Throwable e) {
    List<String> messages = []
    for (Throwable t = e; t != null; t = t.cause) {
      messages << String.valueOf(t.message)
    }
    messages
  }
}
