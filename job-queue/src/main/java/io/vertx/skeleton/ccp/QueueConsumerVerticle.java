package io.vertx.skeleton.ccp;


import io.vertx.skeleton.sql.Repository;
import io.vertx.skeleton.sql.RepositoryHandler;
import io.vertx.skeleton.sql.models.RecordWithoutID;
import io.vertx.skeleton.utils.CustomClassLoader;
import io.vertx.skeleton.rabbitmq.RabbitMQClientBootstrap;
import io.vertx.skeleton.ccp.consumers.*;
import io.vertx.skeleton.ccp.mappers.MessageQueueSql;
import io.vertx.skeleton.ccp.models.*;
import io.vertx.skeleton.ccp.subscribers.PgQueueSubscriber;
import io.vertx.skeleton.ccp.mappers.MessageTransactionMapper;
import io.vertx.skeleton.ccp.subscribers.RabbitMQSubscriber;
import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.MessageState;
import io.vertx.skeleton.models.exceptions.VertxServiceException;
import io.vertx.skeleton.ccp.mappers.MessageQueueMapper;
import io.activej.inject.Injector;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.Message;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;

import java.util.*;

import static io.vertx.skeleton.ccp.models.QueueImplementation.PG_QUEUE;
import static java.util.stream.Collectors.groupingBy;

public class QueueConsumerVerticle extends AbstractVerticle {

  private final Collection<Module> modules;
  private List<PgQueueSubscriber> pgQueueSubscribers = new ArrayList<>();
  private List<RabbitMQSubscriber> rabbitMqSubscribers = new ArrayList<>();
  private io.vertx.mutiny.pgclient.pubsub.PgSubscriber pgSubscriber;
  private RabbitMQClient rabbitMQClient;
  private RepositoryHandler repositoryHandler;
  private final List<TaskQueueHandler> handlers = new ArrayList<>();
  private static final Logger LOGGER = LoggerFactory.getLogger(QueueConsumerVerticle.class);
  private Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue;
  private Repository<MessageTransactionID, MessageTransaction, MessageTransactionQuery> messageTransaction;


  public QueueConsumerVerticle(Collection<Module> modules) {
    this.modules = modules;
  }

  @Override
  public Uni<Void> asyncStart() {
    final Injector injector = getInjector(modules);
    final var queueWrappers = queueWrapper(config(), deploymentID(), injector);
    final var queuesBootstrapping = MessageQueueSql.bootstrapQueue(repositoryHandler);
    return queuesBootstrapping
      .flatMap(avoid -> eventbusProxy())
      .flatMap(avoid -> subscribe(queueWrappers));
  }

  private Injector getInjector(Collection<Module> modules) {
    final var moduleBuilder = ModuleBuilder.create().install(modules);
    this.repositoryHandler = RepositoryHandler.leasePool(config(), vertx);
    this.pgSubscriber = io.vertx.mutiny.pgclient.pubsub.PgSubscriber.subscriber(
      repositoryHandler.vertx(),
      RepositoryHandler.connectionOptions(repositoryHandler.configuration())
    );
    pgSubscriber.reconnectPolicy(integer -> 0L);
    moduleBuilder.bind(RepositoryHandler.class).toInstance(repositoryHandler);
    moduleBuilder.bind(Vertx.class).toInstance(vertx);
    moduleBuilder.bind(JsonObject.class).toInstance(config());
    moduleBuilder.bind(RabbitMQClient.class).toInstance(rabbitMQClient);
    moduleBuilder.bind(io.vertx.mutiny.pgclient.pubsub.PgSubscriber.class).toInstance(pgSubscriber);
    return Injector.of(moduleBuilder.build());
  }

  private Uni<Void> eventbusProxy() {
    return vertx.eventBus().<EvMessageBatch>consumer(QueueConsumerVerticle.class.getName())
      .handler(message -> {
          LOGGER.debug("Message received -> " + message.body());
          final var messages = messages(message);
          this.messageQueue.insertBatch(messages)
            .subscribe()
            .with(
              avoid -> {
                message.reply("Messages correctly handled");
                LOGGER.info("Messages correctly handled");
              },
              throwable -> {
                if (throwable instanceof VertxServiceException vertxServiceException) {
                  message.fail(400, JsonObject.mapFrom(vertxServiceException.error()).encode());
                }
                if (throwable instanceof CompositeException compositeException) {
                  message.fail(400, JsonObject.mapFrom(new Error(
                        compositeException.getMessage(),
                        null,
                        400
                      )
                    ).encode()
                  );
                } else {
                  LOGGER.error("Unexpected error in eventbus proxy", throwable);
                  message.fail(400, JsonObject.mapFrom(new Error(throwable.getMessage(), throwable.getLocalizedMessage(), 400)).encode());
                }
              }
            );
        }
      )
      .exceptionHandler(this::handleException)
      .completionHandler();
  }

  private static List<MessageRecord> messages(Message<EvMessageBatch> message) {
    try {
      return message.body().messages().stream()
        .map(evMessage -> new MessageRecord(
            evMessage.id(),
            evMessage.scheduled(),
            evMessage.expiration(),
            evMessage.priority(),
            0,
            MessageState.CREATED,
            evMessage.payloadClass(),
            JsonObject.mapFrom(evMessage.payload()),
            null,
            null,
            RecordWithoutID.newRecord((evMessage.tenant())
            )
          )
        )
        .toList();
    } catch (Exception exception) {
      message.fail(400, "Unable to parse messages -> " + exception.getMessage());
      throw new VertxServiceException(new Error("Unable to parse messages", "", 400));
    }
  }


  private void handleException(Throwable throwable) {
    LOGGER.error("[-- QueueConsumer had to drop the following exception --]", throwable);
  }

  private Uni<Void> subscribe(List<MessageProcessorWrapper> wrappers) {
    this.messageQueue = new Repository<>(MessageQueueMapper.INSTANCE, repositoryHandler);
    this.messageTransaction = new Repository<>(MessageTransactionMapper.INSTANCE, repositoryHandler);
    final var handler = new TaskQueueHandler(
      wrappers,
      pgSubscriber,
      messageQueue,
      messageTransaction
    );
    if (handler.configuration().queueType() == PG_QUEUE) {
      final var subscriber = new PgQueueSubscriber(messageQueue, pgSubscriber);
      subscriber.subscribe(handler, deploymentID());
      pgQueueSubscribers.add(subscriber);
    }
    return pgSubscriber.connect();
  }

  private RabbitMQClient rabbitMQClient() {
    if (rabbitMQClient == null) {
      this.rabbitMQClient = RabbitMQClientBootstrap.bootstrap(vertx, config());
      return rabbitMQClient;
    }
    return rabbitMQClient;
  }

  @Override
  public Uni<Void> asyncStop() {
    if (!rabbitMqSubscribers.isEmpty()) {
      return Multi.createFrom().iterable(rabbitMqSubscribers)
        .onItem().transformToUniAndMerge(queue -> queue.stop())
        .collect().asList()
        .call(avoid -> rabbitMQClient.stop().replaceWithVoid());
    }
    return Multi.createFrom().iterable(pgQueueSubscribers)
      .onItem().transformToUniAndMerge(PgQueueSubscriber::unsubscribe)
      .collect().asList()
      .flatMap(avoid -> repositoryHandler.shutDown());

  }

  private List<MessageProcessorWrapper> queueWrapper(JsonObject newConfiguration, String deploymentId, Injector injector) {
    final var singleProcessMessageConsumers = CustomClassLoader.loadFromInjector(injector, QueueMessageProcessor.class);
    final var queueMap = new HashMap<Class<?>, List<QueueMessageProcessor>>();
    singleProcessMessageConsumers.forEach(
      impl -> {
        final var tClass = CustomClassLoader.getFirstGenericType(impl);
        if (queueMap.containsKey(tClass)) {
          queueMap.get(tClass).add(impl);
        } else {
          final var param = new ArrayList<QueueMessageProcessor>();
          param.add(impl);
          queueMap.put(tClass, param);
        }
      }
    );
    final var queueWrappers = queueMap.entrySet().stream()
      .map(entry -> {
          final var tClass = entry.getKey();
          final var consumers = CustomClassLoader.loadFromInjector(injector, QueueEventConsumer.class).stream()
            .filter(consumer -> CustomClassLoader.getFirstGenericType(consumer).isAssignableFrom(tClass))
            .toList();
          validateProcessors(entry.getValue(), tClass);
          final var defaultProcessor = entry.getValue().stream().filter(p -> p.tenants() == null).findFirst().orElseThrow();
          final var customProcessors = entry.getValue().stream()
            .filter(p -> p.tenants() != null)
            .collect(groupingBy(QueueMessageProcessor::tenants));
          var configuration = defaultProcessor.configuration();
          final var jsonConfig = newConfiguration.getJsonObject(defaultProcessor.getClass().getSimpleName());
          if (jsonConfig != null) {
            final var jsonQueueConfig = jsonConfig.mapTo(QueueConfiguration.class);
            configuration = mergeConfig(defaultProcessor.configuration(), jsonQueueConfig);
          }
          final Logger logger = LoggerFactory.getLogger(configuration.queueName());
          final var queueWrapper = new MessageProcessorWrapper(
            deploymentId,
            defaultProcessor,
            customProcessors,
            consumers,
            tClass,
            configuration,
            logger
          );
          return queueWrapper;
        }
      )
      .toList();
    return queueWrappers;
  }

  private QueueConfiguration mergeConfig(QueueConfiguration configuration, QueueConfiguration jsonQueueConfig) {
    return configuration
      .setQueueName(Objects.requireNonNullElse(jsonQueueConfig.queueName(), configuration.queueName()))
      .setBatchSize(Objects.requireNonNullElse(jsonQueueConfig.batchSize(), configuration.batchSize()))
      .setIdempotency(Objects.requireNonNullElse(jsonQueueConfig.idempotentProcess(), configuration.idempotentProcess()))
      .setQueueType(Objects.requireNonNullElse(jsonQueueConfig.queueType(), configuration.queueType()))
      .setEmptyBackOffInMinutes(Objects.requireNonNullElse(jsonQueueConfig.emptyBackOffInMinutes(), configuration.emptyBackOffInMinutes()))
      .setErrorBackOffInMinutes(Objects.requireNonNullElse(jsonQueueConfig.errorBackOffInMinutes(), configuration.errorBackOffInMinutes()))
      .setMaxRetry(Objects.requireNonNullElse(jsonQueueConfig.maxRetry(), configuration.maxRetry()))
      .setThrottleInMs(Objects.requireNonNullElse(jsonQueueConfig.throttleInMs(), configuration.throttleInMs()))
      .setMaxProcessingTimeInMinutes(Objects.requireNonNullElse(jsonQueueConfig.maxProcessingTimeInMinutes(), configuration.maxProcessingTimeInMinutes()));
  }

  private void validateProcessors(List<QueueMessageProcessor> queues, Class<?> tClass) {
    if (queues.stream().filter(
      p -> p.tenants() == null
    ).toList().size() > 1) {
      throw new IllegalStateException("More than one default implementation for -> " + tClass);
    }
    queues.stream()
      .filter(p -> p.tenants() != null)
      .collect(groupingBy(QueueMessageProcessor::tenants))
      .forEach((key, value) -> {
          if (value.size() > 1) {
            throw new IllegalStateException("More than one custom implementation for tenant " + key + " queue -> " + tClass);
          }
        }
      );
  }

  public static <T, R> void logQueueConfiguration(final MessageProcessorWrapper<T, R> messageProcessorWrapper) {
    final var customProcessors = new JsonObject();
    messageProcessorWrapper.customProcessors()
      .forEach((key, value) -> key.forEach(k -> customProcessors.put(k.generateString(), value.getClass().getName())));
    final var customConsumers = new JsonObject();
    messageProcessorWrapper.consumers()
      .stream()
      .filter(c -> c.tenants() != null)
      .forEach(
        c -> c.tenants().forEach(t -> customConsumers.put(t.generateString(), c.getClass().getName()))
      );
    final var defaultConsumers = messageProcessorWrapper.consumers()
      .stream()
      .filter(c -> c.tenants() == null)
      .map(c -> c.getClass().getName())
      .toList();
    final var queueConfiguration = new JsonObject()
      .put("defaultProcessor", messageProcessorWrapper.defaultProcessor().getClass().getName())
      .put("customProcessors", customProcessors)
      .put("defaultConsumers", defaultConsumers)
      .put("customConsumers", customConsumers)
      .put("payloadClass", messageProcessorWrapper.payloadClass().getName())
      .put("configuration", JsonObject.mapFrom(messageProcessorWrapper.configuration()));
    LOGGER.info("Queue configured -> " + queueConfiguration.encodePrettily());
  }

}
