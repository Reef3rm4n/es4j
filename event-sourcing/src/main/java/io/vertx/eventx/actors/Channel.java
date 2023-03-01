package io.vertx.eventx.actors;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.FixedDemandPacer;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.eventx.cache.Actions;
import io.vertx.eventx.cache.ChannelAddress;
import io.vertx.eventx.exceptions.CommandRejected;
import io.vertx.eventx.exceptions.NodeNotFound;
import io.vertx.eventx.exceptions.UnknownCommand;
import io.vertx.eventx.objects.Action;
import io.vertx.eventx.storage.pg.models.AggregateKey;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.Message;
import io.vertx.mutiny.core.eventbus.MessageConsumer;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.common.EventXError;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

import static io.vertx.core.eventbus.ReplyFailure.RECIPIENT_FAILURE;
import static io.vertx.eventx.cache.ChannelAddress.commandConsumer;

public class Channel {
  private Channel() {
  }

  public static final String ACTION = "action";
  public static final String CLASS_NAME = "className";
  private static final Logger LOGGER = LoggerFactory.getLogger(Channel.class);
  private static final HashRing<SimpleNode> ACTOR_HASH_RING = HashRing.<SimpleNode>newBuilder()
    .name("entity-aggregate-hash-ring")       // set hash ring name
    .hasher(DefaultHasher.MURMUR_3)   // hash function to distribute partitions
    .partitionRate(100000)                  // number of partitions per node
    .build();

  // todo put a pipe in the channel that routes commands from the eventbus to the correct handler.
  public static <T extends Aggregate> Uni<Void> createChannel(Vertx vertx, Class<T> entityClass, String deploymentID) {
    return invokeConsumer(vertx, entityClass, deploymentID)
      .flatMap(avoid -> broadcastConsumer(vertx, entityClass))
      .flatMap(avoid -> commandBridge(vertx, entityClass)
        .completionHandler()
      );
  }

  private static <T extends Aggregate> MessageConsumer<JsonObject> commandBridge(Vertx vertx, Class<T> entityClass) {
    return vertx.eventBus().<JsonObject>consumer(ChannelAddress.commandBridge(entityClass))
      .exceptionHandler(throwable -> handlerThrowable(throwable, entityClass))
      .handler(message -> request(
          vertx,
          entityClass,
          message.body(),
          Action.valueOf(Objects.requireNonNull(message.headers().get(ACTION), "Missing action headers, either COMMAND or LOAD"))
        )
      );
  }

  private static <T extends Aggregate> Uni<Void> broadcastConsumer(Vertx vertx, Class<T> entityClass) {
    return vertx.eventBus().<String>consumer(ChannelAddress.broadcastChannel(entityClass))
      .handler(objectMessage -> synchronizeChannel(objectMessage, entityClass))
      .exceptionHandler(throwable -> handlerThrowable(throwable, entityClass))
      .completionHandler()
      .flatMap(avoid -> Multi.createBy().repeating().supplier(() -> ACTOR_HASH_RING.getNodes().isEmpty())
        .atMost(10).capDemandsTo(1).paceDemand()
        .using(new FixedDemandPacer(1, Duration.ofMillis(500)))
        .collect().last()
        .map(Unchecked.function(
            aBoolean -> {
              if (Boolean.TRUE.equals(aBoolean)) {
                throw new NodeNotFound(new EventXError("Hash ring empty", "Hash ring synchronizer was still empty after 10 seconds, there's no entity deployed in the platform", -999));
              }
              return aBoolean;
            }
          )
        )
      )
      .replaceWithVoid();
  }

  private static <T extends Aggregate> Uni<Void> invokeConsumer(Vertx vertx, Class<T> entityClass, String deploymentID) {
    return vertx.eventBus().<String>consumer(ChannelAddress.invokeChannel(entityClass))
      .handler(stringMessage -> broadcastActorAddress(vertx, entityClass, deploymentID))
      .exceptionHandler(throwable -> handlerThrowable(throwable, entityClass))
      .completionHandler()
      .replaceWithVoid();
  }

  public static <T extends Aggregate> void broadcastActorAddress(Vertx vertx, Class<T> entityClass, String deploymentID) {
    LOGGER.info("New command consumer " + entityClass.getSimpleName() + " [address: " + commandConsumer(entityClass, deploymentID) + "]");
    vertx.eventBus().<String>publish(
      ChannelAddress.broadcastChannel(entityClass),
      commandConsumer(entityClass, deploymentID),
      new DeliveryOptions()
        .setLocalOnly(false)
        .setTracingPolicy(TracingPolicy.ALWAYS)
        .addHeader(Actions.ACTION.name(), Actions.ADD.name())
    );
  }

  public static <T extends Aggregate> void killActor(Vertx vertx, Class<T> entityClass, String deploymentID) {
    vertx.eventBus().publish(
      ChannelAddress.broadcastChannel(entityClass),
      commandConsumer(entityClass, deploymentID),
      new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.REMOVE.name())
    );
  }

  public static <T extends Aggregate> void invokeActorsBroadcast(Class<T> entityClass, Vertx vertx) {
    vertx.eventBus().publish(
      ChannelAddress.invokeChannel(entityClass),
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
    LOGGER.error("[-- " + entityClass.getSimpleName() + " had to drop the following exception --]", throwable);
  }

  private static void addActor(final String actorAddress) {
    final var simpleNode = SimpleNode.of(actorAddress);
    if (!ACTOR_HASH_RING.contains(simpleNode)) {
      LOGGER.info("Adding actor to hash-ring [address:" + actorAddress + "]");
      ACTOR_HASH_RING.add(simpleNode);
    } else {
      LOGGER.info("Actor already in hash-ring [address:" + actorAddress + "]");
    }
  }

  public static <T extends Aggregate> Uni<T> request(Vertx vertx, Class<T> entityClass, JsonObject payload, Action action) {
    final var aggregateKey = new AggregateKey(Objects.requireNonNull(payload.getString("aggregateId")), payload.getJsonObject("headers").getString("tenantId", "default"));
    final var address = Channel.resolveActor(entityClass, aggregateKey);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Proxying command -> " + new JsonObject()
        .put("key", aggregateKey)
        .put("address", address)
        .put("payload", payload)
        .encodePrettily()
      );
    }
    return vertx.eventBus().<JsonObject>request(
        address,
        payload,
        new DeliveryOptions()
          .setLocalOnly(false)
          .setSendTimeout(250)
          .addHeader(ACTION, action.name())
      )
      .onFailure(ReplyException.class).retry().atMost(3)
      .map(response -> response.body().mapTo(entityClass))
      .onFailure().transform(Unchecked.function(Channel::transformError));
  }

  private static Throwable transformError(final Throwable throwable) {
    if (throwable instanceof ReplyException reply) {
      LOGGER.error("Reply from handler -> ", reply);
      if (reply.failureType() == RECIPIENT_FAILURE) {
        try {
          final var error = new JsonObject(reply.getLocalizedMessage()).mapTo(EventXError.class);
          return new CommandRejected(error);
        } catch (IllegalArgumentException illegalArgument) {
          LOGGER.error("Unable to parse rejectCommand -> ", illegalArgument);
          return new CommandRejected(new EventXError(throwable.getMessage(), null, 500));
        }
      } else {
        return new CommandRejected(new EventXError(reply.failureType().name(), reply.getMessage(), reply.failureCode()));
      }
    } else {
      LOGGER.error("Unknown exception from handler -> ", throwable);
      return new CommandRejected(new EventXError(throwable.getMessage(), null, 500));
    }
  }

  public static <T extends Aggregate> String resolveActor(Class<T> entityClass, AggregateKey key) {
    final var node = ACTOR_HASH_RING.locate(entityClass.getSimpleName() + key.aggregateId());
    return node.orElse(ACTOR_HASH_RING.getNodes().stream().findFirst()
        .orElseThrow(() -> new NodeNotFound(key.aggregateId()))
      )
      .getKey();
  }

  private static void handlerThrowable(final Throwable throwable, Class<?> entityClass) {
    LOGGER.error("[-- Channel for entity " + entityClass.getSimpleName() + " had to drop the following exception --]", throwable);
  }

  private static void removeActor(final String handler) {
    final var simpleNode = SimpleNode.of(handler);
    if (ACTOR_HASH_RING.contains(simpleNode)) {
      LOGGER.info("Removing actor form hash-ring [address: " + handler + "]");
      ACTOR_HASH_RING.remove(simpleNode);
    } else {
      LOGGER.info("Actor not present in hash-ring [address: " + handler + "]");
    }
  }

  private static void synchronizeChannel(Message<String> objectMessage, Class<?> entityID) {
    LOGGER.debug("Synchronizing " + entityID.getSimpleName() + " Channel " + new JsonObject()
      .put("action", objectMessage.headers().get(Actions.ACTION.name()))
      .put("body", objectMessage.body()).encodePrettily());
    switch (Actions.valueOf(objectMessage.headers().get(Actions.ACTION.name()))) {
      case ADD -> addActor(objectMessage.body());
      case REMOVE -> removeActor(objectMessage.body());
      default -> throw UnknownCommand.unknown(objectMessage.body().getClass());
    }
  }


}
