package io.es4j.core.verticles;


import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;

import org.crac.Context;
import org.crac.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.es4j.infrastructure.misc.Es4jServiceLoader;
import io.es4j.queue.MessageProcessor;
import io.es4j.queue.TaskSubscriber;
import io.es4j.queue.QueueTransactionManager;
import io.es4j.queue.exceptions.QueueError;
import io.es4j.queue.exceptions.MessageException;
import io.es4j.queue.models.MessageProcessorManager;
import io.es4j.queue.models.MessageProcessorWrapper;
import io.es4j.queue.models.QueueConfiguration;
import io.es4j.queue.postgres.PgTaskSubscriber;
import io.es4j.queue.postgres.PgQueueTransaction;
import io.vertx.mutiny.core.Vertx;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static java.util.stream.Collectors.groupingBy;

public class TaskProcessorVerticle extends AbstractVerticle implements Resource {
  private final List<MessageProcessor> messageProcessors;

  @Override
  public void beforeCheckpoint(Context<? extends Resource> context) throws Exception {
    Promise<Void> p = Promise.promise();
    stop(p);
    CountDownLatch latch = new CountDownLatch(1);
    p.future().onComplete(event -> latch.countDown());
    latch.await();
  }

  @Override
  public void afterRestore(Context<? extends Resource> context) throws Exception {
    Promise<Void> p = Promise.promise();
    start(p);
    CountDownLatch latch = new CountDownLatch(1);
    p.future().onComplete(event -> latch.countDown());
    latch.await();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskProcessorVerticle.class);
  private TaskSubscriber subscriber;
  private QueueConfiguration taskConfiguration;

  public TaskProcessorVerticle() {
    this.messageProcessors = ServiceLoader.load(MessageProcessor.class).stream().map(ServiceLoader.Provider::get).toList();
    ;
    org.crac.Core.getGlobalContext().register(this);
  }


  public static Uni<Void> deploy(
    final Vertx vertx,
    final JsonObject newConfiguration
  ) {
    return Uni.createFrom().voidItem();
  }

  @Override
  public Uni<Void> asyncStop() {
    return subscriber.unsubscribe();
  }

  @Override
  public Uni<Void> asyncStart() {
    this.taskConfiguration = config().getJsonObject("task-queue", new JsonObject()).mapTo(QueueConfiguration.class);
    QueueTransactionManager queueTransactionManager = getTransactionManager();
    this.subscriber = getSubscriber();
    return subscriber.subscribe(new MessageProcessorManager(
          taskConfiguration,
          bootstrapProcessors(this.deploymentID()),
          queueTransactionManager,
          vertx
        )
      )
      .replaceWithVoid();
  }

  private QueueTransactionManager getTransactionManager() {
    return switch (taskConfiguration.transactionManagerImplementation()) {
      case VERTX_PG_CLIENT -> new PgQueueTransaction(vertx, config());
    };
  }

  private TaskSubscriber getSubscriber() {
    return switch (taskConfiguration.queueImplementation()) {
      case PG_QUEUE -> new PgTaskSubscriber(vertx, config());
      case RABBITMQ ->
        throw new MessageException(new QueueError("queue type not supported", "rabbit task queue is not yet implemented", 999));
      case SOLACE ->
        throw new MessageException(new QueueError("queue type not supported", "solace task queue is not yet implemented", 999));
    };
  }


  public List<MessageProcessorWrapper> bootstrapProcessors(String deploymentId) {
    final var queueMap = new HashMap<Class<?>, List<MessageProcessor>>();
    messageProcessors.forEach(
      impl -> {
        final var tClass = Es4jServiceLoader.getFirstGenericType(impl);
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
            throw new IllegalStateException("More than one custom implementation for tenant " + key + " queue -> " + tClass);
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
