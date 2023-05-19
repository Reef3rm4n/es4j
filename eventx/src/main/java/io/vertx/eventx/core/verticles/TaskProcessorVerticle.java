package io.vertx.eventx.core.verticles;

import io.activej.inject.Injector;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import org.crac.Context;
import org.crac.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.infrastructure.misc.CustomClassLoader;
import io.vertx.eventx.queue.MessageProcessor;
import io.vertx.eventx.queue.TaskSubscriber;
import io.vertx.eventx.queue.QueueTransactionManager;
import io.vertx.eventx.queue.exceptions.QueueError;
import io.vertx.eventx.queue.exceptions.MessageException;
import io.vertx.eventx.queue.models.MessageProcessorManager;
import io.vertx.eventx.queue.models.MessageProcessorWrapper;
import io.vertx.eventx.queue.models.QueueConfiguration;
import io.vertx.eventx.queue.postgres.PgTaskSubscriber;
import io.vertx.eventx.queue.postgres.PgQueueTransaction;
import io.vertx.mutiny.core.Vertx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static io.vertx.eventx.launcher.EventxMain.MAIN_MODULES;
import static java.util.stream.Collectors.groupingBy;

public class TaskProcessorVerticle extends AbstractVerticle implements Resource {
  @Override
  public void beforeCheckpoint(Context<? extends Resource> context) throws Exception {

  }

  @Override
  public void afterRestore(Context<? extends Resource> context) throws Exception {

  }
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskProcessorVerticle.class);
  private final Collection<Module> modules;
  private TaskSubscriber subscriber;
  private Injector injector;
  private QueueTransactionManager queueTransactionManager;
  private QueueConfiguration taskConfiguration;

  public TaskProcessorVerticle(Collection<Module> modules) {
    this.modules = modules;
  }


  public static Uni<Void> deploy(
    final Vertx vertx,
    final JsonObject newConfiguration
  ) {
    if (CustomClassLoader.checkPresenceInModules(MessageProcessor.class, MAIN_MODULES)) {
      return vertx.deployVerticle(
        () -> new TaskProcessorVerticle(MAIN_MODULES),
        new DeploymentOptions()
          .setInstances(CpuCoreSensor.availableProcessors() * 2)
          .setConfig(newConfiguration)
      ).replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }

  @Override
  public Uni<Void> asyncStop() {
    return subscriber.unsubscribe();
  }

  @Override
  public Uni<Void> asyncStart() {
    this.taskConfiguration = config().getJsonObject("task-queue", JsonObject.mapFrom(new QueueConfiguration())).mapTo(QueueConfiguration.class);
    this.injector = bindModules(modules);
    this.queueTransactionManager = getTransactionManager();
    this.subscriber = getSubscriber();
    return subscriber.subscribe(new MessageProcessorManager(
          taskConfiguration,
          bootstrapProcessors(this.deploymentID(), injector),
          queueTransactionManager,
          vertx
        )
      )
      .replaceWithVoid();
  }

  private QueueTransactionManager getTransactionManager() {
    return switch (taskConfiguration.transactionManagerImplementation()) {
      case VERTX_PG_CLIENT -> new PgQueueTransaction(injector);
    };
  }

  private TaskSubscriber getSubscriber() {
    return switch (taskConfiguration.queueImplementation()) {
      case PG_QUEUE -> new PgTaskSubscriber(injector);
      case RABBITMQ ->
        throw new MessageException(new QueueError("queue type not supported", "rabbit task queue is not yet implemented", 999));
      case SOLACE ->
        throw new MessageException(new QueueError("queue type not supported", "solace task queue is not yet implemented", 999));
    };
  }

  private Injector bindModules(Collection<Module> modules) {
    final var moduleBuilder = ModuleBuilder.create().install(modules);
    moduleBuilder.bind(Vertx.class).toInstance(vertx);
    moduleBuilder.bind(JsonObject.class).toInstance(config());
    moduleBuilder.bind(QueueConfiguration.class).toInstance(taskConfiguration);
    return Injector.of(moduleBuilder.build());
  }


  public List<MessageProcessorWrapper> bootstrapProcessors(String deploymentId, Injector injector) {
    final var singleProcessMessageConsumers = CustomClassLoader.loadFromInjector(injector, MessageProcessor.class);
    final var queueMap = new HashMap<Class<?>, List<MessageProcessor>>();
    singleProcessMessageConsumers.forEach(
      impl -> {
        final var tClass = CustomClassLoader.getFirstGenericType(impl);
        if (queueMap.containsKey(tClass)) {
          queueMap.get(tClass).add(impl);
        } else {
          final var param = new ArrayList<MessageProcessor>();
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
            .collect(groupingBy(MessageProcessor::tenants));
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

  private static void validateProcessors(List<MessageProcessor> queues, Class<?> tClass) {
    if (queues.stream().filter(
      p -> p.tenants() == null
    ).toList().size() > 1) {
      throw new IllegalStateException("More than one default implementation for -> " + tClass);
    }
    queues.stream()
      .filter(p -> p.tenants() != null)
      .collect(groupingBy(MessageProcessor::tenants))
      .forEach((key, value) -> {
          if (value.size() > 1) {
            throw new IllegalStateException("More than one custom implementation for tenantId " + key + " queue -> " + tClass);
          }
        }
      );
  }

  public static <T> void logQueueConfiguration(final MessageProcessorWrapper<T> messageProcessorWrapper, QueueConfiguration queueConfiguration) {
    final var customProcessors = new JsonObject();
    messageProcessorWrapper.customProcessors()
      .forEach((key, value) -> key.forEach(k -> customProcessors.put(k, value.getClass().getName())));
    final var json = new JsonObject()
      .put("defaultProcessor", messageProcessorWrapper.defaultProcessor().getClass().getName())
      .put("customProcessors", customProcessors.encodePrettily())
      .put("payloadClass", messageProcessorWrapper.payloadClass().getName())
      .put("configuration", JsonObject.mapFrom(queueConfiguration).encodePrettily());
    LOGGER.info("Queue configuration {} ", json.encodePrettily());
  }

}
