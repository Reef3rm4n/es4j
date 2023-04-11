package io.vertx.eventx.infrastructure.bus;

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
import io.vertx.eventx.infrastructure.models.AggregatePlainKey;
import io.vertx.eventx.exceptions.CommandRejected;
import io.vertx.eventx.exceptions.NodeUnavailable;
import io.vertx.eventx.exceptions.UnknownCommand;
import io.vertx.eventx.objects.Action;
import io.vertx.eventx.objects.AggregateState;
import io.vertx.eventx.objects.ErrorSource;
import io.vertx.eventx.objects.EventxError;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.Message;
import io.vertx.eventx.Aggregate;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static io.vertx.core.eventbus.ReplyFailure.RECIPIENT_FAILURE;
import static io.vertx.eventx.infrastructure.bus.AddressResolver.commandConsumer;

public class AggregateBus {
  private AggregateBus() {
  }

  public static final String ACTION = "action";
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateBus.class);

  private static HashRing<SimpleNode> startHashRing(Class<? extends Aggregate> aggregateClass) {
    return HashRing.<SimpleNode>newBuilder()
      .name(aggregateClass.getName())       // set hash ring name
      .hasher(DefaultHasher.MURMUR_3)   // hash function to distribute partitions
      .partitionRate(100000)                  // number of partitions per node
      .build();
  }

  public static final Map<Class<? extends Aggregate>, HashRing<SimpleNode>> HASH_RING_MAP = new HashMap<>();

  // todo put a pipe in the channel that routes commands from the eventbus to the correct handler.
  public static <T extends Aggregate> Uni<Void> eventbusBridge(Vertx vertx, Class<T> aggregateClass, String deploymentID) {
    HASH_RING_MAP.put(aggregateClass, startHashRing(aggregateClass));
    return vertx.eventBus().<String>consumer(AddressResolver.invokeChannel(aggregateClass))
      .handler(stringMessage -> broadcastActorAddress(vertx, aggregateClass, deploymentID))
      .exceptionHandler(throwable -> handlerThrowable(throwable, aggregateClass))
      .completionHandler()
      .call(avoid -> vertx.eventBus().<JsonObject>consumer(AddressResolver.commandBridge(aggregateClass))
        .exceptionHandler(throwable -> handlerThrowable(throwable, aggregateClass))
        .handler(message -> request(
            vertx,
            aggregateClass,
            message.body(),
            Action.valueOf(Objects.requireNonNull(message.headers().get(ACTION), "Missing action headers, either COMMAND or LOAD"))
          )
        )
        .completionHandler()
      )
      .call(avoid -> vertx.eventBus().<String>consumer(AddressResolver.broadcastChannel(aggregateClass))
        .handler(objectMessage -> synchronizeChannel(objectMessage, aggregateClass))
        .exceptionHandler(throwable -> handlerThrowable(throwable, aggregateClass))
        .completionHandler()
      );
  }


  public static <T extends Aggregate> Uni<Void> waitForRegistration(String deploymentID, Class<T> entityClass) {
    return Multi.createBy().repeating().supplier(() -> HASH_RING_MAP.get(entityClass).getNodes()
        .stream().filter(node -> node.getKey().equals(commandConsumer(entityClass, deploymentID)))
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
    LOGGER.info("New command consumer " + entityClass.getSimpleName() + " [address: " + commandConsumer(entityClass, deploymentID) + "]");
    vertx.eventBus().<String>publish(
      AddressResolver.broadcastChannel(entityClass),
      commandConsumer(entityClass, deploymentID),
      new DeliveryOptions()
        .setLocalOnly(false)
        .setTracingPolicy(TracingPolicy.ALWAYS)
        .addHeader(Actions.ACTION.name(), Actions.ADD.name())
    );
  }

  public static <T extends Aggregate> void stop(Vertx vertx, Class<T> entityClass, String deploymentID) {
    vertx.eventBus().publish(
      AddressResolver.broadcastChannel(entityClass),
      commandConsumer(entityClass, deploymentID),
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
    Vertx vertx,
    Class<T> entityClass,
    String deploymentID,
    Consumer<Message<JsonObject>> consumer
  ) {

    return vertx.eventBus().<JsonObject>consumer(commandConsumer(entityClass, deploymentID))
      .handler(consumer)
      .exceptionHandler(throwable -> dropped(entityClass, throwable))
      .completionHandler()
      .invoke(avoid -> broadcastActorAddress(vertx, entityClass, deploymentID));
  }

  private static void dropped(Class<?> entityClass, final Throwable throwable) {
    LOGGER.error("[-- {} Channel had to drop the following exception --]", entityClass.getSimpleName(), throwable);
  }

  private static void addNode(final String actorAddress, HashRing<SimpleNode> hashRing) {
    final var simpleNode = SimpleNode.of(actorAddress);
    if (!hashRing.contains(simpleNode)) {
      LOGGER.debug("Adding {} to {} hash-ring", hashRing.getName(), actorAddress);
      hashRing.add(simpleNode);
    } else {
      LOGGER.debug("{} already present in hash-ring {}", actorAddress, hashRing.getName());
    }
  }

  public static <T extends Aggregate> Uni<AggregateState<T>> request(Vertx vertx, Class<T> aggregateClass, JsonObject payload, Action action) {
    final var command = payload.getJsonObject("command");
    final var aggregateKey = new AggregatePlainKey(
      aggregateClass.getName(),
      Objects.requireNonNull(command.getString("aggregateId")),
      command.getJsonObject("headers").getString("tenantId", "default")
    );
    final var address = AggregateBus.resolveActor(aggregateClass, aggregateKey);
    LOGGER.debug("Proxying {} {}", action.name(), new JsonObject()
      .put("key", aggregateKey)
      .put("address", address)
      .put("payload", payload)
      .encodePrettily()
    );
    return vertx.eventBus().<JsonObject>request(
        address,
        payload,
        new DeliveryOptions()
          .setLocalOnly(false)
          .setSendTimeout(5000)
          .addHeader(ACTION, action.name())
      )
      //.onFailure(throwable -> checkError(throwable)).retry().atMost(3)
      .map(response -> AggregateState.fromJson(response.body(), aggregateClass))
      .onFailure().transform(Unchecked.function(AggregateBus::transformError));
  }


  private static Throwable transformError(final Throwable throwable) {
    if (throwable instanceof ReplyException reply) {
      LOGGER.error("Handler raised error", reply);
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

  public static <T extends Aggregate> String resolveActor(Class<T> entityClass, AggregatePlainKey key) {
    final var node = HASH_RING_MAP.get(entityClass).locate(entityClass.getSimpleName() + key.aggregateId());
    return node.orElse(HASH_RING_MAP.get(entityClass).getNodes().stream().findFirst()
        .orElseThrow(() -> new NodeUnavailable(key.aggregateId()))
      )
      .getKey();
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
    LOGGER.debug(
      "Synchronizing {} {}",
      entityClass.getSimpleName(),
      new JsonObject()
        .put("action", objectMessage.headers().get(Actions.ACTION.name()))
        .put("body", objectMessage.body())
        .encodePrettily()
    );
    HASH_RING_MAP.computeIfAbsent(entityClass, (aClass) -> HASH_RING_MAP.put(aClass, startHashRing(aClass)));
    switch (Actions.valueOf(objectMessage.headers().get(Actions.ACTION.name()))) {
      case ADD -> addNode(objectMessage.body(), HASH_RING_MAP.get(entityClass));
      case REMOVE -> removeActor(objectMessage.body(), HASH_RING_MAP.get(entityClass));
      default -> throw UnknownCommand.unknown(objectMessage.body().getClass());
    }
  }


}
