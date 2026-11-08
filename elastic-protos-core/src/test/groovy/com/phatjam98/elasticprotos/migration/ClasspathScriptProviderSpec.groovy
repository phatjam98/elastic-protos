package com.phatjam98.elasticprotos.migration

import spock.lang.Specification
import spock.lang.Unroll

class ClasspathScriptProviderSpec extends Specification {

  @Unroll
  def "looks up #from -> #to in order"() {
    expect:
    MigrationScriptProvider.classpath().script('lookup', from, to).map { it.trim() }.orElse(null) == found

    where:
    from     | to  | found
    'a'      | 'b' | 'exact'
    'x'      | 'b' | 'to'
    'x'      | 'y' | 'default'
    null     | 'c' | 'legacy'
    'legacy' | 'c' | 'legacy'
  }

  def "lists its candidates"() {
    expect:
    new ClasspathScriptProvider().candidates('docs', null, 'h2') ==
        ['migrations/docs/legacy-h2.painless', 'migrations/docs/h2.painless',
         'migrations/docs/default.painless']
    new ClasspathScriptProvider('/custom/').candidates('docs', 'h1', 'h2')[0] ==
        'custom/docs/h1-h2.painless'
    new ClasspathScriptProvider('').candidates('docs', 'h1', 'h2')[0] == 'docs/h1-h2.painless'
    new ClasspathScriptProvider(null).candidates('docs', 'h1', 'h2')[0] == 'docs/h1-h2.painless'
  }

  def "finds nothing for unknown aliases, other prefixes or none()"() {
    expect:
    !MigrationScriptProvider.classpath().script('unknown', 'a', 'b').isPresent()
    !MigrationScriptProvider.classpath('elsewhere').script('lookup', 'a', 'b').isPresent()
    !MigrationScriptProvider.none().script('lookup', 'a', 'b').isPresent()
  }

  def "uses an explicit class loader"() {
    expect:
    new ClasspathScriptProvider('migrations', getClass().classLoader)
        .script('lookup', 'a', 'b').get().trim() == 'exact'
  }

  def "falls back to its own class loader without a context class loader"() {
    given:
    Thread thread = Thread.currentThread()
    ClassLoader original = thread.contextClassLoader
    thread.contextClassLoader = null

    when:
    def script = new ClasspathScriptProvider().script('lookup', 'a', 'b')

    then:
    script.get().trim() == 'exact'

    cleanup:
    thread.contextClassLoader = original
  }

  def "read failures surface as UncheckedIOException"() {
    given:
    ClassLoader broken = new ClassLoader(null) {
      @Override
      InputStream getResourceAsStream(String name) {
        new InputStream() {
          @Override
          int read() throws IOException {
            throw new IOException('disk on fire')
          }
        }
      }
    }

    when:
    new ClasspathScriptProvider('migrations', broken).script('lookup', 'a', 'b')

    then:
    thrown(UncheckedIOException)
  }
}
