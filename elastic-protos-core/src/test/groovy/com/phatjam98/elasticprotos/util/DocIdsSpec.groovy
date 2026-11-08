package com.phatjam98.elasticprotos.util

import spock.lang.Specification

class DocIdsSpec extends Specification {

  def "join concatenates components with hyphens (#testCase)"() {
    expect:
    DocIds.join(*components) == expected

    where:
    components                       | expected              | testCase
    ["singleArgument"]               | "singleArgument"      | "single component"
    ["arg1", "arg2", "arg3", "arg4"] | "arg1-arg2-arg3-arg4" | "several components"
    ["a", "b", "c"]                  | "a-b-c"               | "three components"
  }

  def "hash is the hex MD5 of the joined components (#testCase)"() {
    expect:
    DocIds.hash(*components) == expected

    where:
    components                       | expected                             | testCase
    ["singleArgument"]               | "6028bd6790f2774400ebfc6bf346147e" | "single component"
    ["arg1", "arg2", "arg3", "arg4"] | "3873fffef53c690f96087adc7f743989" | "several components"
    ["a", "b", "c"]                  | "7b193b3d33184464106f41ddf733783b" | "three components"
  }

  def "is a static utility with a private constructor"() {
    given:
    def constructor = DocIds.getDeclaredConstructor()

    expect:
    java.lang.reflect.Modifier.isPrivate(constructor.modifiers)

    when:
    constructor.accessible = true
    constructor.newInstance()

    then:
    noExceptionThrown()
  }
}
