package com.phatjam98.elasticprotos.query

import com.phatjam98.elasticprotos.mapping.IndexDefinition
import com.phatjam98.elasticprotos.mapping.ProtoMapper
import com.phatjam98.elasticprotos.proto.example.Firm
import com.phatjam98.elasticprotos.query.NestedPathResolver.Clause
import com.phatjam98.elasticprotos.query.NestedPathResolver.Occur
import spock.lang.Shared
import spock.lang.Specification

class NestedPathResolverSpec extends Specification {

  @Shared IndexDefinition firm = ProtoMapper.withDefaults().map(Firm)

  def "resolves and wraps nested fields"() {
    given:
    def resolver = new NestedPathResolver(firm, true)

    expect:
    resolver.ancestors('offices.address.city') == ['offices']
    resolver.innermost('offices.name') == 'offices'
    resolver.innermost('headquarters.city') == null
    resolver.wrap('offices.name', [match_all: [:]]) == [nested: [path: 'offices', query: [match_all: [:]]]]
    resolver.wrap('name', [match_all: [:]]) == [match_all: [:]]
    NestedPathResolver.wrap(['a', 'a.b'], [x: 1]) ==
        [nested: [path: 'a', query: [nested: [path: 'a.b', query: [x: 1]]]]]
    Occur.MUST_NOT.key() == 'must_not'
  }

  def "groups keep their position and pick filter or must"() {
    given:
    def resolver = new NestedPathResolver(firm, true)

    when:
    Map bool = resolver.bool([
        new Clause(Occur.FILTER, ['offices'], [a: 1]),
        new Clause(Occur.FILTER, [], [b: 2]),
        new Clause(Occur.FILTER, ['offices'], [c: 3])])

    then:
    bool == [bool: [filter: [
        [nested: [path: 'offices', query: [bool: [filter: [[a: 1], [c: 3]]]]]],
        [b: 2]]]]
    new Clause(Occur.MUST, [], [:]).innermost() == null
  }
}
