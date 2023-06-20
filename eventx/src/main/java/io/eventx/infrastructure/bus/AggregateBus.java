package io.eventx.infrastructure.bus;

import io.eventx.Aggregate;
import io.eventx.Command;
import io.eventx.core.exceptions.EventxException;
import io.eventx.core.objects.AggregateState;
import io.eventx.core.objects.EventxError;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.FixedDemandPacer;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.ReplyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.core.tracing.TracingPolicy;
import io.eventx.infrastructure.models.AggregatePlainKey;
import io.eventx.core.exceptions.CommandRejected;
import io.eventx.core.exceptions.NodeUnavailable;
import io.eventx.core.exceptions.UnknownCommand;
import io.eventx.core.objects.ErrorSource;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.Message;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static io.eventx.infrastructure.bus.AddressResolver.commandBridge;
import static io.vertx.core.eventbus.ReplyFailure.RECIPIENT_FAILURE;
import static io.eventx.infrastructure.bus.AddressResolver.commandConsumer;

public class AggregateBus {
  public static final String COMMAND_BRIDGE = "command-bridge";

  private AggregateBus() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateBus.class);

  private static HashRing<SimpleNode> startHashRing(Class<? extends Aggregate> aggregateClass) {
    return HashRing.<SimpleNode>newBuilder()
      .name(aggregateClass.getName())       // set hash ring fileName
      .hasher(DefaultHasher.MURMUR_3)   // hash function to distribute partitions
      .partitionRate(100000)                  // number of partitions per node
      .build();
  }

  public static final Map<Class<? extends Aggregate>, HashRing<SimpleNode>> HASH_RING_MAP = new HashMap<>();

  public static <T extends Aggregate> Uni<Void> startChannel(Vertx vertx, Class<T> aggregateClass, String deploymentID) {
    HASH_RING_MAP.put(aggregateClass, startHashRing(aggregateClass));
    return vertx.eventBus().<String>consumer(AddressResolver.invokeChannel(aggregateClass))
      .handler(stringMessage -> broadcastActorAddress(vertx, aggregateClass, deploymentID))
      .exceptionHandler(throwable -> handlerThrowable(throwable, aggregateClass))
      .completionHandler()
      .call(avoid -> vertx.eventBus().<String>consumer(AddressResolver.broadcastChannel(aggregateClass))
        .handler(objectMessage -> synchronizeChannel(objectMessage, aggregateClass))
        .exceptionHandler(throwable -> handlerThrowable(throwable, aggregateClass))
        .completionHandler()
      );
  }


  public static <T extends Aggregate> Uni<Void> waitForRegistration(String deploymentID, Class<T> entityClass) {
    return Multi.createBy().repeating().supplier(() -> HASH_RING_MAP.get(entityClass).getNodes()
        .stream().filter(node -> node.getKey().equals(AddressResolver.nodeAddress(entityClass, deploymentID)))
        .toList().isEmpty()
      )
      .atMost(10).capDemandsTo(1).paceDemand()
      .using(new FixedDemandPacer(1, Duration.ofMillis(500)))
      .collect().last()
      .map(Unchecked.function(
          aBoolean -> {
            if (Boolean.TRUE.equals(aBoolean)) {
              throw new NodeUnavailable(new EventxError(
                null,
                "Hash ring synchronizer was still empty after 10 seconds",
                null,
                null,
                null,
                null
              )
              );
            }
            return aBoolean;
          }
        )
      )
      .replaceWithVoid();
  }

  public static <T extends Aggregate> void broadcastActorAddress(Vertx vertx, Class<T> entityClass, String deploymentID) {
    LOGGER.info("Publishing [{}] address[{}] ", entityClass.getSimpleName(), AddressResolver.nodeAddress(entityClass, deploymentID));
    vertx.eventBus().<String>publish(
      AddressResolver.broadcastChannel(entityClass),
      AddressResolver.nodeAddress(entityClass, deploymentID),
      new DeliveryOptions()
        .setLocalOnly(false)
        .setTracingPolicy(TracingPolicy.ALWAYS)
        .addHeader(Actions.ACTION.name(), Actions.ADD.name())
    );
  }

  public static <T extends Aggregate> void stop(
    final Vertx vertx,
    final Class<T> entityClass,
    final String deploymentID
  ) {
    vertx.eventBus().publish(
      AddressResolver.broadcastChannel(entityClass),
      AddressResolver.nodeAddress(entityClass, deploymentID),
      new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.REMOVE.name())
    );
  }

  public static <T extends Aggregate> void invokeActorsBroadcast(Class<T> entityClass, Vertx vertx) {
    vertx.eventBus().publish(
      AddressResolver.invokeChannel(entityClass),
      "",
      new DeliveryOptions().setLocalOnly(false)
    );
  }

  public static <T extends Aggregate> Uni<Void> registerCommandConsumer(
    final Vertx vertx,
    final Class<T> aggregateClass,
    final String deploymentID,
    final Consumer<Message<JsonObject>> consumer,
    final Class<? extends Command> commandClass
  ) {
    return  registerEventBusBridge(vertx, aggregateClass, commandClass)
      .flatMap(avoid -> registerEventbusCommandConsumer(vertx, aggregateClass, deploymentID, consumer, commandClass));
  }

  private static <T extends Aggregate> Uni<Void> registerEventbusCommandConsumer(Vertx vertx, Class<T> aggregateClass, String deploymentID, Consumer<Message<JsonObject>> consumer, Class<? extends Command> commandClass) {
    return vertx.eventBus().<JsonObject>consumer(commandConsumer(aggregateClass, deploymentID, commandClass))
      .handler(consumer)
      .exceptionHandler(throwable -> dropped(aggregateClass, throwable, commandClass))
      .completionHandler()
      .invoke(avoid -> broadcastActorAddress(vertx, aggregateClass, deploymentID));
  }

  private static <T extends Aggregate> Uni<Void> registerEventBusBridge(Vertx vertx, Class<T> aggregateClass, Class<? extends Command> commandClass) {
    return vertx.eventBus().<JsonObject>consumer(commandBridge(aggregateClass, commandClass))
      .handler(message -> {
        final var command = message.body().mapTo(commandClass);
        request(vertx, aggregateClass, command)
          .subscribe()
          .with(
            jsonBody -> message.reply(jsonBody.toJson()),
            throwable -> {
              if (throwable instanceof EventxException eventxException) {
                message.fail(eventxException.error().externalErrorCode(), JsonObject.mapFrom(eventxException.error()).encode());
              } else {
                message.fail(400, throwable.getMessage());
              }
            }
          );
      })
      .exceptionHandler(throwable -> dropped(aggregateClass, throwable, commandClass))
      .completionHandler();
  }

  private static void dropped(Class<?> entityClass, final Throwable throwable, Class<? extends Command> commandClass) {
    LOGGER.error("[-- {} channel had to drop an exception during the handling of command {} --]", entityClass.getSimpleName(), commandClass.getName(), throwable);
  }

  private static void addNode(final String actorAddress, final HashRing<SimpleNode> hashRing) {
    final var simpleNode = SimpleNode.of(actorAddress);
    if (!hashRing.contains(simpleNode)) {
      LOGGER.debug("Adding {} to {} hash-ring", hashRing.getName(), actorAddress);
      hashRing.add(simpleNode);
    }
  }

  public static <T extends Aggregate> Uni<AggregateState<T>> request(
    final Vertx vertx,
    final Class<T> aggregateClass,
    final Command command
  ) {
    final var aggregateKey = new AggregatePlainKey(
      aggregateClass.getName(),
      command.aggregateId(),
      command.tenant()
    );
    final var encodedCommand = JsonObject.mapFrom(command);
    final var address = AggregateBus.resolveNode(aggregateClass, aggregateKey, command);
    LOGGER.debug("Proxying  {} -> {}", address, encodedCommand.encodePrettily());
    return vertx.eventBus().<JsonObject>request(
        address,
        encodedCommand,
        new DeliveryOptions()
          .setTracingPolicy(TracingPolicy.ALWAYS)
          .setLocalOnly(!vertx.isClustered())
          .setSendTimeout(2000)
      )
      .map(response -> AggregateState.fromJson(response.body(), aggregateClass))
      .onFailure().transform(Unchecked.function(AggregateBus::transformError));
  }


  private static Throwable transformError(final Throwable throwable) {
    if (throwable instanceof ReplyException reply) {
      if (reply.failureType() == RECIPIENT_FAILURE) {
        try {
          final var error = new JsonObject(reply.getLocalizedMessage()).mapTo(EventxError.class);
          return new CommandRejected(error);
        } catch (IllegalArgumentException illegalArgument) {
          LOGGER.error("Unable to parse command", illegalArgument);
          return new CommandRejected(new EventxError(
            throwable.getMessage(),
            null,
            500
          )
          );
        }
      } else {
        return new CommandRejected(new EventxError(
          ErrorSource.INFRASTRUCTURE,
          AggregateBus.class.getName(),
          reply.getMessage(),
          reply.failureType().name(),
          String.valueOf(reply.failureCode()),
          500
        )
        );
      }
    } else {
      LOGGER.error("Unknown exception from handler", throwable);
      return new CommandRejected(new EventxError(throwable.getMessage(), null, 500));
    }
  }

  public static <T extends Aggregate> String resolveNode(Class<T> entityClass, AggregatePlainKey key, Command command) {
    final var node = HASH_RING_MAP.get(entityClass).locate(entityClass.getSimpleName() + key.aggregateId());
    final var nodeAddress = node.orElse(HASH_RING_MAP.get(entityClass).getNodes().stream().findFirst()
        .orElseThrow(() -> new NodeUnavailable(key.aggregateId()))
      )
      .getKey();
    return AddressResolver.resolveCommandConsumer(nodeAddress, command.getClass());
  }

  private static void handlerThrowable(final Throwable throwable, Class<?> entityClass) {
    LOGGER.error("[-- Channel for entity {} had to drop the following exception --]", entityClass.getSimpleName(), throwable);
  }

  private static void removeActor(final String handler, HashRing<SimpleNode> hashRing) {
    final var simpleNode = SimpleNode.of(handler);
    if (hashRing.contains(simpleNode)) {
      LOGGER.info("Removing {} form hash-ring {}", handler, hashRing.getName());
      hashRing.remove(simpleNode);
    } else {
      LOGGER.info("{} not present in hash-ring {}", handler, hashRing.getName());
    }
  }

  private static void synchronizeChannel(Message<String> objectMessage, Class<? extends Aggregate> entityClass) {
    HASH_RING_MAP.computeIfAbsent(entityClass, (aClass) -> HASH_RING_MAP.put(aClass, startHashRing(aClass)));
    switch (Actions.valueOf(objectMessage.headers().get(Actions.ACTION.name()))) {
      case ADD -> addNode(objectMessage.body(), HASH_RING_MAP.get(entityClass));
      case REMOVE -> removeActor(objectMessage.body(), HASH_RING_MAP.get(entityClass));
      default -> throw UnknownCommand.unknown(objectMessage.body().getClass());
    }
  }


}
