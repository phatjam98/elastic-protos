package com.phatjam98.elasticprotos.micronaut;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.phatjam98.elasticprotos.engine.SearchEngine;
import com.phatjam98.elasticprotos.mapping.ProtoMapper;
import com.phatjam98.elasticprotos.migration.IndexMigrator;
import com.phatjam98.elasticprotos.migration.MigrationPlan;
import com.phatjam98.elasticprotos.migration.MigrationResult;
import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates or migrates the indices of the classes listed in {@code
 * elastic-protos.bootstrap.classes} when the application starts.
 *
 * <p>The plan report is logged at INFO. When {@code elastic-protos.bootstrap.fail-on-error} is
 * true (the default) and an index failed or was blocked, a {@link BootstrapException} is thrown
 * from the startup event, which aborts {@code ApplicationContext.start()}. Disable the bootstrap
 * with {@code elastic-protos.bootstrap.enabled: false}, for example on all but one instance.
 */
@Singleton
@Requires(beans = SearchEngine.class)
@Requires(property = "elastic-protos.bootstrap.enabled", notEquals = "false")
public class ElasticProtosBootstrap implements ApplicationEventListener<StartupEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticProtosBootstrap.class);

  private final IndexMigrator migrator;
  private final ElasticProtosConfiguration config;
  private final ClassLoader classLoader;
  private final AtomicBoolean done = new AtomicBoolean();

  /**
   * Creates the listener.
   *
   * @param migrator the migrator
   * @param config the configuration
   * @param beanContext the context, whose class loader loads the configured classes
   */
  public ElasticProtosBootstrap(IndexMigrator migrator, ElasticProtosConfiguration config,
      BeanContext beanContext) {
    this.migrator = migrator;
    this.config = config;
    this.classLoader = beanContext.getClassLoader();
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    if (done.compareAndSet(false, true)) {
      run();
    }
  }

  /**
   * Runs the bootstrap now.
   *
   * @return the result, or null when no classes are configured
   * @throws BootstrapException when a class cannot be loaded, or when the result is not ok and
   *     {@code fail-on-error} is set
   */
  public MigrationResult run() {
    ElasticProtosConfiguration.Bootstrap bootstrap = config.getBootstrap();
    List<String> names = bootstrap.getClasses();
    if (names == null || names.isEmpty()) {
      LOG.info("elastic-protos bootstrap skipped: elastic-protos.bootstrap.classes is empty");
      return null;
    }
    MigrationPlan plan = migrator.plan(load(names));
    LOG.info("elastic-protos bootstrap plan{}:\n{}",
        bootstrap.isDryRun() ? " (dry run)" : "", plan.toReport());
    MigrationResult result = migrator.apply(plan);
    if (result.ok()) {
      LOG.info("elastic-protos bootstrap finished: {} index(es) ok", result.outcomes().size());
      return result;
    }
    String problems = describe(result);
    if (bootstrap.isFailOnError()) {
      throw new BootstrapException("elastic-protos bootstrap failed: " + problems,
          firstError(result));
    }
    LOG.warn("elastic-protos bootstrap finished with problems (fail-on-error is false): {}",
        problems);
    return result;
  }

  @SuppressWarnings("unchecked")
  private List<Descriptor> load(List<String> names) {
    List<Descriptor> types = new ArrayList<>();
    for (String name : names) {
      Class<?> type;
      try {
        type = Class.forName(name.trim(), true, classLoader);
      } catch (ClassNotFoundException | LinkageError e) {
        throw new BootstrapException("elastic-protos.bootstrap.classes: cannot load class "
            + name.trim(), e);
      }
      if (!Message.class.isAssignableFrom(type)) {
        throw new BootstrapException("elastic-protos.bootstrap.classes: " + name.trim()
            + " is not a generated protobuf message class", null);
      }
      types.add(ProtoMapper.descriptorOf((Class<? extends Message>) type));
    }
    return types;
  }

  private static String describe(MigrationResult result) {
    List<String> parts = new ArrayList<>();
    for (MigrationResult.Outcome outcome : result.problems()) {
      StringBuilder part = new StringBuilder()
          .append(outcome.plan().alias())
          .append(' ')
          .append(outcome.plan().action())
          .append('/')
          .append(outcome.status());
      if (outcome.plan().reason() != null) {
        part.append(": ").append(outcome.plan().reason());
      }
      if (outcome.error() != null) {
        part.append(" (").append(outcome.error().getMessage()).append(')');
      }
      parts.add(part.toString());
    }
    return String.join(", ", parts);
  }

  private static Throwable firstError(MigrationResult result) {
    return result.problems().stream()
        .map(MigrationResult.Outcome::error)
        .filter(error -> error != null)
        .findFirst()
        .orElse(null);
  }

  /** Thrown when the startup bootstrap cannot load its classes or fails to migrate an index. */
  public static class BootstrapException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param message what went wrong
     * @param cause the underlying failure, or null
     */
    public BootstrapException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
