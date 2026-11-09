package com.phatjam98.elasticprotos.mapping

import spock.lang.Specification

class IndexNamingStrategySpec extends Specification {

  def "the default lock index is shared by every strategy that does not override it"() {
    given:
    IndexNamingStrategy custom = { base, hash -> base + '-' + hash } as IndexNamingStrategy

    expect:
    IndexNamingStrategy.DEFAULT.lockIndex() == 'elastic-protos-locks'
    custom.lockIndex() == 'elastic-protos-locks'
  }

  def "a prefixed strategy prefixes the lock index like its aliases"() {
    expect:
    IndexNamingStrategy.prefixed('Dev').lockIndex() == 'dev-elastic-protos-locks'
    IndexNamingStrategy.prefixed('it-1234').lockIndex() == 'it-1234-elastic-protos-locks'
    IndexNamingStrategy.prefixed('dev').alias('documents') == 'dev-documents'
  }
}
