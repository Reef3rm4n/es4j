package io.vertx.skeleton.ccp;


import io.vertx.skeleton.utils.CustomClassLoader;
import io.vertx.skeleton.rabbitmq.RabbitMQClientBootstrap;
import io.vertx.skeleton.ccp.consumers.*;
import io.vertx.skeleton.ccp.mappers.MessageQueueSql;
import io.vertx.skeleton.ccp.models.*;
import io.vertx.skeleton.ccp.subscribers.PgQueueSubscriber;
import io.vertx.skeleton.ccp.mappers.MessageTransactionMapper;
import io.vertx.skeleton.ccp.subscribers.RabbitMQSubscriber;
import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.PersistedRecord;
import io.vertx.skeleton.models.MessageState;
import io.vertx.skeleton.models.exceptions.VertxServiceException;
import io.vertx.skeleton.orm.Repository;
import io.vertx.skeleton.orm.RepositoryHandler;
import io.vertx.skeleton.ccp.mappers.MessageQueueMapper;
import io.activej.inject.Injector;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.Message;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;

import java.util.*;
import java.util.stream.Stream;

import static io.vertx.skeleton.ccp.models.QueueType.PG_QUEUE;
import static java.util.stream.Collectors.groupingBy;

public class QueueConsumerVerticle extends AbstractVerticle {

  private final Collection<Module> modules;
  private List<PgQueueSubscriber> pgQueueSubscribers = new ArrayList<>();
  private List<RabbitMQSubscriber> rabbitMqSubscribers = new ArrayList<>();
  private io.vertx.mutiny.pgclient.pubsub.PgSubscriber pgSubscriber;
  private RabbitMQClient rabbitMQClient;
  private RepositoryHandler repositoryHandler;
  private final List<Tuple2<SingleProcessHandler, Repository<MessageRecordID, MessageRecord, MessageRecordQuery>>> handlers = new ArrayList<>();

  private static final Logger LOGGER = LoggerFactory.getLogger(QueueConsumerVerticle.class);


  public QueueConsumerVerticle(Collection<Module> modules) {
    this.modules = modules;
  }

  @Override
  public Uni<Void> asyncStart() {
    final Injector injector = getInjector(modules);
    final var singleWrappers = singleWrapper(config(), deploymentID(), injector);
    final var multiWrappers = multiWrapper(config(), deploymentID(), injector);
    final var queueConfigurations = Stream.concat(
        singleWrappers.stream().map(SingleWrapper::configuration),
        multiWrappers.stream().map(MultiWrapper::configuration)
      )
      .toList();
    final var queuesBootstrapping = MessageQueueSql.bootstrapQueues(repositoryHandler, queueConfigurations);
    return queuesBootstrapping
      .flatMap(avoid -> eventbusProxy())
      .flatMap(avoid -> subscribe(singleWrappers, multiWrappers));
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
          final var queue = queue(message);
          final Tuple2<SingleProcessHandler, Repository<MessageRecordID, MessageRecord, MessageRecordQuery>> handler = handlerTuple(queue, message);
          final var messages = messages(message);
          final var highPriorityMessages = messages.stream()
            .filter(m -> m.scheduled() == null)
            .filter(m -> m.priority() == null || m.priority() <= 0)
            .toList();
          processHighPriorityMessages(handler, highPriorityMessages)
            .flatMap(avoid -> {
                final var nonHighPriorityMessages = messages.stream()
                  .filter(m -> highPriorityMessages.stream().noneMatch(mm -> mm.id().equals(m.id())))
                  .toList();
                if (!nonHighPriorityMessages.isEmpty()) {
                  return handler.getItem2().insertBatch(nonHighPriorityMessages);
                }
                return Uni.createFrom().voidItem();
              }
            )
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

  private static String queue(Message<EvMessageBatch> message) {
    try {
      return Objects.requireNonNull(message.headers().get("queue"), "queue must not be null");
    } catch (Exception exception) {
      message.fail(400, "Unable to define queue -> " + exception.getMessage());
      throw new VertxServiceException(new Error("Unable to define queue", "", 400));
    }
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
            JsonObject.mapFrom(evMessage.payload()),
            null,
            null,
            PersistedRecord.newRecord(evMessage.tenant())
          )
        )
        .toList();
    } catch (Exception exception) {
      message.fail(400, "Unable to parse messages -> " + exception.getMessage());
      throw new VertxServiceException(new Error("Unable to parse messages", "", 400));
    }
  }

  private Tuple2<SingleProcessHandler, Repository<MessageRecordID, MessageRecord, MessageRecordQuery>> handlerTuple(String queue, Message<EvMessageBatch> message) {
    try {
      return handlers.stream().filter(h -> h.getItem1().configuration().queueName().equalsIgnoreCase(queue))
        .findFirst().orElseThrow();
    } catch (Exception exception) {
      message.fail(400, "Unable to find handler -> " + exception.getMessage());
      throw new VertxServiceException(new Error("Unalbe to find handler", "", 400));
    }
  }

  private static Uni<Void> processHighPriorityMessages(Tuple2<SingleProcessHandler, Repository<MessageRecordID, MessageRecord, MessageRecordQuery>> handler, List<MessageRecord> highPriorityMessages) {
    if (!highPriorityMessages.isEmpty()) {
      handler.getItem1().logger().info("Handling high priority messages");
      return handler.getItem1().process(highPriorityMessages);
    }
    return Uni.createFrom().voidItem();
  }

  private void endHandler() {

  }

  private void handleException(Throwable throwable) {
    LOGGER.error("[-- QueueConsumer had to drop the following exception --]", throwable);
  }

  private Uni<Void> subscribe(List<SingleWrapper> wrappers, List<MultiWrapper> multiProcessConsumers) {
    wrappers.forEach(
      wrapper -> {
        final var queue = new Repository<>(
          new MessageQueueMapper(wrapper.configuration().queueName()),
          repositoryHandler
        );
        final var queueIdempotency = new Repository<>(
          new MessageTransactionMapper(wrapper.configuration().queueName() + "_tx"),
          repositoryHandler
        );
        final var singleProcess = new SingleProcessHandler<>(
          wrapper,
          pgSubscriber,
          queue,
          queueIdempotency
        );
        if (singleProcess.configuration().queueType() == PG_QUEUE) {
          handlers.add(Tuple2.of(singleProcess, queue));
          final var subscriber = new PgQueueSubscriber(queue, pgSubscriber);
          subscriber.subscribe(singleProcess, deploymentID());
          pgQueueSubscribers.add(subscriber);
        }
      }
    );
    multiProcessConsumers.forEach(
      wrapper -> {
        final var queue = new Repository<>(
          new MessageQueueMapper(wrapper.configuration().queueName()),
          repositoryHandler
        );
        final var queueIdempotency = new Repository<>(
          new MessageTransactionMapper(wrapper.configuration().queueName() + "_tx"),
          repositoryHandler
        );
        final var mutiProcess = new MutiProcessHandler<>(
          wrapper,
          queue,
          queueIdempotency
        );
        if (mutiProcess.configuration().queueType() == PG_QUEUE) {
          final var subscriber = new PgQueueSubscriber(queue, pgSubscriber);
          subscriber.subscribe(mutiProcess, deploymentID());
          pgQueueSubscribers.add(subscriber);
        }
      }
    );
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

  private List<MultiWrapper> multiWrapper(JsonObject newConfiguration, String deploymentId, Injector injector) {
    final var singleProcessMessageConsumers = CustomClassLoader.loadFromInjector(injector, MultiProcessConsumer.class);
    final var queueMap = new HashMap<Class<?>, List<MultiProcessConsumer>>();
    singleProcessMessageConsumers.forEach(
      impl -> {
        final var tClass = CustomClassLoader.getFirstGenericType(impl);
        if (queueMap.containsKey(tClass)) {
          queueMap.get(tClass).add(impl);
        } else {
          final var param = new ArrayList<MultiProcessConsumer>();
          param.add(impl);
          queueMap.put(tClass, param);
        }
      }
    );
    final var multiProcessWrappers = queueMap.entrySet().stream()
      .map(entry -> {
          final var tClass = entry.getKey();
          final var defaultProcessors = entry.getValue().stream()
            .filter(p -> p.tenants() == null)
            .toList();
          final var configuration = entry.getValue().stream()
            .filter(p -> p.tenants() == null)
            .filter(p -> p.configuration() != null)
            .findFirst()
            .map(MultiProcessConsumer::configuration)
            .orElseThrow(() -> new IllegalStateException("No configuration for multi-process queue"));
          final var customProcessors = entry.getValue().stream()
            .filter(p -> p.tenants() != null)
            .collect(groupingBy(MultiProcessConsumer::tenants));
          final Logger logger = LoggerFactory.getLogger(configuration.queueName());
          final var queueWrapper = new MultiWrapper(
            deploymentId,
            defaultProcessors,
            customProcessors,
            tClass,
            configuration,
            logger
          );
          return queueWrapper;
        }
      )
      .toList();
    return multiProcessWrappers;
  }

  private List<SingleWrapper> singleWrapper(JsonObject newConfiguration, String deploymentId, Injector injector) {
    final var singleProcessMessageConsumers = CustomClassLoader.loadFromInjector(injector, SingleProcessConsumer.class);
    final var queueMap = new HashMap<Class<?>, List<SingleProcessConsumer>>();
    singleProcessMessageConsumers.forEach(
      impl -> {
        final var tClass = CustomClassLoader.getFirstGenericType(impl);
        if (queueMap.containsKey(tClass)) {
          queueMap.get(tClass).add(impl);
        } else {
          final var param = new ArrayList<SingleProcessConsumer>();
          param.add(impl);
          queueMap.put(tClass, param);
        }
      }
    );
    final var singleProcessConsumerWrappers = queueMap.entrySet().stream()
      .map(entry -> {
          final var tClass = entry.getKey();
          final var consumers = CustomClassLoader.loadFromInjector(injector, ProcessEventConsumer.class).stream()
            .filter(consumer -> CustomClassLoader.getFirstGenericType(consumer).isAssignableFrom(tClass))
            .toList();
          validateProcessors(entry.getValue(), tClass);
          final var defaultProcessor = entry.getValue().stream().filter(p -> p.tenants() == null).findFirst().orElseThrow();
          final var customProcessors = entry.getValue().stream()
            .filter(p -> p.tenants() != null)
            .collect(groupingBy(SingleProcessConsumer::tenants));
          var configuration = defaultProcessor.configuration();
          final var jsonConfig = newConfiguration.getJsonObject(defaultProcessor.getClass().getSimpleName());
          if (jsonConfig != null) {
            final var jsonQueueConfig = jsonConfig.mapTo(QueueConfiguration.class);
            configuration = mergeConfig(defaultProcessor.configuration(), jsonQueueConfig);
          }
          final Logger logger = LoggerFactory.getLogger(configuration.queueName());
          final var queueWrapper = new SingleWrapper(
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
    return singleProcessConsumerWrappers;
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

  private void validateProcessors(List<SingleProcessConsumer> queues, Class<?> tClass) {
    if (queues.stream().filter(
      p -> p.tenants() == null
    ).toList().size() > 1) {
      throw new IllegalStateException("More than one default implementation for -> " + tClass);
    }
    queues.stream()
      .filter(p -> p.tenants() != null)
      .collect(groupingBy(SingleProcessConsumer::tenants))
      .forEach((key, value) -> {
          if (value.size() > 1) {
            throw new IllegalStateException("More than one custom implementation for tenant " + key + " queue -> " + tClass);
          }
        }
      );
  }

  public static <T, R> void logQueueConfiguration(final SingleWrapper<T, R> singleWrapper) {
    final var customProcessors = new JsonObject();
    singleWrapper.customProcessors()
      .forEach((key, value) -> key.forEach(k -> customProcessors.put(k.generateString(), value.getClass().getName())));
    final var customConsumers = new JsonObject();
    singleWrapper.consumers()
      .stream()
      .filter(c -> c.tenants() != null)
      .forEach(
        c -> c.tenants().forEach(t -> customConsumers.put(t.generateString(), c.getClass().getName()))
      );
    final var defaultConsumers = singleWrapper.consumers()
      .stream()
      .filter(c -> c.tenants() == null)
      .map(c -> c.getClass().getName())
      .toList();
    final var queueConfiguration = new JsonObject()
      .put("defaultProcessor", singleWrapper.defaultProcessor().getClass().getName())
      .put("customProcessors", customProcessors)
      .put("defaultConsumers", defaultConsumers)
      .put("customConsumers", customConsumers)
      .put("payloadClass", singleWrapper.payloadClass().getName())
      .put("configuration", JsonObject.mapFrom(singleWrapper.configuration()));
    LOGGER.info("Queue configured -> " + queueConfiguration.encodePrettily());
  }

}
