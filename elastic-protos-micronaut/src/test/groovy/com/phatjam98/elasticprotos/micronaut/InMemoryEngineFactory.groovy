package com.phatjam98.elasticprotos.micronaut

import com.phatjam98.elasticprotos.testkit.InMemorySearchEngine
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import jakarta.inject.Singleton

/**
 * A user-defined engine bean. It carries neither @Primary nor @Replaces: it wins over the
 * factory's @Secondary beans by being an ordinary bean of the same type.
 */
@Factory
@Requires(property = 'test.engine', value = 'memory')
class InMemoryEngineFactory {

  @Singleton
  @Bean(preDestroy = 'close')
  InMemorySearchEngine inMemorySearchEngine() {
    new InMemorySearchEngine()
  }
}

/** Pre-creates a concrete index named like an alias, so that the bootstrap plan is BLOCKED. */
@Singleton
@Requires(property = 'test.preexisting-index')
class PreexistingIndexListener implements BeanCreatedEventListener<InMemorySearchEngine> {

  @Value('${test.preexisting-index}')
  String name

  @Override
  InMemorySearchEngine onCreated(BeanCreatedEvent<InMemorySearchEngine> event) {
    event.bean.createIndex(name, [:], [properties: [:]], [] as Set)
    event.bean
  }
}
