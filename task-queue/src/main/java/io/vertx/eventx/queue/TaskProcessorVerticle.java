package io.vertx.eventx.queue;


import io.vertx.core.DeploymentOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.eventx.queue.exceptions.TaskProcessorException;
import io.vertx.eventx.queue.postgres.PgTransaction;
import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.eventx.common.CustomClassLoader;
import io.vertx.eventx.queue.models.*;
import io.vertx.eventx.queue.postgres.PgTaskSubscriber;
import io.activej.inject.Injector;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.util.*;

import static java.util.stream.Collectors.groupingBy;

public class TaskProcessorVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskProcessorVerticle.class);
  private final Collection<Module> modules;
  private TaskSubscriber subscriber;
  private Injector injector;
  private TransactionManager transactionManager;
  private TaskQueueConfiguration taskConfiguration;

  public TaskProcessorVerticle(Collection<Module> modules) {
    this.modules = modules;
  }


  public static Uni<Void> deploy(
    final Vertx vertx,
    final JsonObject newConfiguration,
    final Collection<Module> modules
  ) {
    if (CustomClassLoader.checkPresenceInModules(TaskProcessor.class, modules)) {
      LOGGER.info("Deploying queue consumers ...");
      return vertx.deployVerticle(
        () -> new TaskProcessorVerticle(modules),
        new DeploymentOptions()
          .setInstances(CpuCoreSensor.availableProcessors() * 2)
          .setConfig(newConfiguration)
      ).replaceWithVoid();
    }
    LOGGER.info("Skipping task-queue, no implementations found in modules");
    return Uni.createFrom().voidItem();
  }

  @Override
  public Uni<Void> asyncStop() {
    return subscriber.unsubscribe();
  }

  @Override
  public Uni<Void> asyncStart() {
    this.taskConfiguration = config().getJsonObject("task-queue", JsonObject.mapFrom(new TaskQueueConfiguration())).mapTo(TaskQueueConfiguration.class);
    this.injector = bindModules(modules);
    this.transactionManager = getTransactionManager();
    this.subscriber = getSubscriber();
    return subscriber.subscribe(new TaskProcessorManager(
          taskConfiguration,
          bootstrapProcessors(this.deploymentID(), injector),
          transactionManager,
          vertx
        )
      )
      .replaceWithVoid();
  }

  private TransactionManager getTransactionManager() {
    return switch (taskConfiguration.transactionManagerImplementation()) {
      case JOOQ ->
        throw new TaskProcessorException(new EventXError("transaction manager not supported", "jooq transaction manager is not yet implemented", 999));
      case VERTX_PG_CLIENT -> new PgTransaction(injector);
    };
  }

  private TaskSubscriber getSubscriber() {
    return switch (taskConfiguration.queueImplementation()) {
      case PG_QUEUE -> new PgTaskSubscriber(injector);
      case RABBITMQ ->
        throw new TaskProcessorException(new EventXError("queue type not supported", "rabbit task queue is not yet implemented", 999));
      case SOLACE ->
        throw new TaskProcessorException(new EventXError("queue type not supported", "solace task queue is not yet implemented", 999));
    };
  }

  private Injector bindModules(Collection<Module> modules) {
    final var repositoryHandler = RepositoryHandler.leasePool(config(), vertx);
    final var moduleBuilder = ModuleBuilder.create().install(modules);
    moduleBuilder.bind(RepositoryHandler.class).toInstance(repositoryHandler);
    moduleBuilder.bind(Vertx.class).toInstance(vertx);
    moduleBuilder.bind(JsonObject.class).toInstance(config());
    moduleBuilder.bind(TaskQueueConfiguration.class).toInstance(taskConfiguration);
    return Injector.of(moduleBuilder.build());
  }


  public List<MessageProcessorWrapper> bootstrapProcessors(String deploymentId, Injector injector) {
    final var singleProcessMessageConsumers = CustomClassLoader.loadFromInjector(injector, TaskProcessor.class);
    final var queueMap = new HashMap<Class<?>, List<TaskProcessor>>();
    singleProcessMessageConsumers.forEach(
      impl -> {
        final var tClass = CustomClassLoader.getFirstGenericType(impl);
        if (queueMap.containsKey(tClass)) {
          queueMap.get(tClass).add(impl);
        } else {
          final var param = new ArrayList<TaskProcessor>();
          param.add(impl);
          queueMap.put(tClass, param);
        }
      }
    );
    return queueMap.entrySet().stream()
      .map(entry -> {
          final var tClass = entry.getKey();
          validateProcessors(entry.getValue(), tClass);
          final var defaultProcessor = entry.getValue().stream().filter(p -> p.tenants() == null).findFirst().orElseThrow();
          final var customProcessors = entry.getValue().stream()
            .filter(p -> p.tenants() != null)
            .collect(groupingBy(TaskProcessor::tenants));
          final var queueWrapper = new MessageProcessorWrapper(
            deploymentId,
            defaultProcessor,
            customProcessors,
            tClass
          );
          logQueueConfiguration(queueWrapper, taskConfiguration);
          return queueWrapper;
        }
      )
      .toList();
  }

  private static void validateProcessors(List<TaskProcessor> queues, Class<?> tClass) {
    if (queues.stream().filter(
      p -> p.tenants() == null
    ).toList().size() > 1) {
      throw new IllegalStateException("More than one default implementation for -> " + tClass);
    }
    queues.stream()
      .filter(p -> p.tenants() != null)
      .collect(groupingBy(TaskProcessor::tenants))
      .forEach((key, value) -> {
          if (value.size() > 1) {
            throw new IllegalStateException("More than one custom implementation for tenantId " + key + " queue -> " + tClass);
          }
        }
      );
  }

  public static <T> void logQueueConfiguration(final MessageProcessorWrapper<T> messageProcessorWrapper, TaskQueueConfiguration taskQueueConfiguration) {
    final var customProcessors = new JsonObject();
    messageProcessorWrapper.customProcessors()
      .forEach((key, value) -> key.forEach(k -> customProcessors.put(k, value.getClass().getName())));
    final var json = new JsonObject()
      .put("defaultProcessor", messageProcessorWrapper.defaultProcessor().getClass().getName())
      .put("customProcessors", customProcessors)
      .put("payloadClass", messageProcessorWrapper.payloadClass().getName())
      .put("configuration", JsonObject.mapFrom(taskQueueConfiguration));
    LOGGER.info("Queue configured -> " + json.encodePrettily());
  }

}
