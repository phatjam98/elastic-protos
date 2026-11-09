package com.phatjam98.elasticprotos

import spock.lang.Requires
import spock.lang.Specification

/**
 * Guards the protobuf 4.x CI job: with {@code -PprotobufRuntimeVersion=<v>} the build passes
 * {@code elasticprotos.protobufRuntime=<v>} to the test JVM, and this spec proves the forced runtime
 * is the one actually loaded. Skipped in ordinary runs.
 */
@Requires({ System.getProperty('elasticprotos.protobufRuntime') })
class ProtobufRuntimeSpec extends Specification {

  def "the forced protobuf-java runtime is the one on the test classpath"() {
    given:
    String requested = System.getProperty('elasticprotos.protobufRuntime')
    int requestedMajor = Integer.parseInt(requested.tokenize('.')[0])

    when:
    Class<?> runtimeVersion = Class.forName('com.google.protobuf.RuntimeVersion')

    then:
    runtimeVersion.getField('MAJOR').getInt(null) == requestedMajor
  }
}
