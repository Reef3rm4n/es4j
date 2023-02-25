package io.vertx.skeleton.evs.actors;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.FixedDemandPacer;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.Message;
import io.vertx.skeleton.evs.Entity;
import io.vertx.skeleton.evs.cache.Actions;
import io.vertx.skeleton.evs.cache.ChannelAddress;
import io.vertx.skeleton.evs.exceptions.NodeNotFoundException;
import io.vertx.skeleton.evs.objects.EntityKey;
import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.CacheException;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.time.Duration;

public class Channel {
  private Channel() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Channel.class);
  private static final HashRing<SimpleNode> actorHashRing = HashRing.<SimpleNode>newBuilder()
    .name("entity-aggregate-hash-ring")       // set hash ring name
    .hasher(DefaultHasher.MURMUR_3)   // hash function to distribute partitions
    .partitionRate(100000)                  // number of partitions per node
    .build();

  public static <T extends Entity> Uni<Void> registerActor(Vertx vertx, Class<T> entityClass, String deploymentID) {
    // publishes heart beats for aggregate handlers.
    return invokeChannelConsumer(vertx, entityClass, deploymentID)
      .flatMap(avoid -> broadcastListener(vertx, entityClass));
  }

  private static <T extends Entity> Uni<Void> broadcastListener(Vertx vertx, Class<T> entityClass) {
    return vertx.eventBus().<String>consumer(ChannelAddress.broadcastChannel(entityClass))
      .handler(Channel::synchronizeChannel)
      .exceptionHandler(throwable -> handlerThrowable(throwable, entityClass))
      .completionHandler()
      .flatMap(avoid -> Multi.createBy().repeating().supplier(() -> actorHashRing.getNodes().isEmpty())
        .atMost(10).capDemandsTo(1).paceDemand()
        .using(new FixedDemandPacer(1, Duration.ofMillis(500)))
        .collect().last()
        .map(Unchecked.function(
            aBoolean -> {
              if (Boolean.TRUE.equals(aBoolean)) {
                throw new NodeNotFoundException(new Error("Hash ring empty", "Hash ring synchronizer was still empty after 10 seconds, there's no entity deployed in the platform", -999));
              }
              return aBoolean;
            }
          )
        )
      )
      .replaceWithVoid();
  }

  private static <T extends Entity> Uni<Void> invokeChannelConsumer(Vertx vertx, Class<T> entityClass, String deploymentID) {
    return vertx.eventBus().<String>consumer(ChannelAddress.invokeChannel(entityClass))
      .handler(stringMessage -> broadcast(vertx, entityClass, deploymentID))
      .exceptionHandler(throwable -> handlerThrowable(throwable, entityClass))
      .completionHandler()
      .replaceWithVoid();
  }

  public static <T extends Entity> void broadcast(Vertx vertx, Class<T> entityClass, String deploymentID) {
    LOGGER.info("Publishing entity " + entityClass.getSimpleName() + " [address: " + Channel.actorAddress(entityClass, deploymentID) + "]");
    vertx.eventBus().<String>publish(
      ChannelAddress.broadcastChannel(entityClass),
      actorAddress(entityClass, deploymentID),
      new DeliveryOptions()
        .setTracingPolicy(TracingPolicy.ALWAYS)
        .addHeader(Actions.ACTION.name(), Actions.ADD.name())
    );
  }

  public static <T extends Entity> void killActor(Vertx vertx, Class<T> entityClass, String deploymentID) {
    vertx.eventBus().publish(
      ChannelAddress.broadcastChannel(entityClass),
      actorAddress(entityClass, deploymentID),
      new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.REMOVE.name())
    );
  }

  public static <T extends Entity> void invokeBroadCast(Class<T> entityClass, Vertx vertx) {
    vertx.eventBus().publish(
      ChannelAddress.invokeChannel(entityClass),
      "null",
      new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.REMOVE.name())
    );
  }

  public static String actorAddress(Class<? extends Entity> entityClass, String deploymendID) {
    return entityClass.getSimpleName() + "-" + deploymendID;
  }

  private static void addHandler(final String actorAddress) {
    final var simpleNode = SimpleNode.of(actorAddress);
    if (!actorHashRing.contains(simpleNode)) {
      LOGGER.info("Adding actor to hash-ring [address:" + actorAddress + "]");
      actorHashRing.add(simpleNode);
    } else {
      LOGGER.info("Actor already in hash-ring [address:" + actorAddress + "]");
    }
  }

  public static <T extends Entity> String resolveActor(Class<T> entityClass, EntityKey key) {
    LOGGER.info("Looking for handler for entity -> " + key);
    final var node = actorHashRing.locate(entityClass.getSimpleName() + key.entityId());
    LOGGER.info("Resolved to node -> " + node);
    return node.orElse(actorHashRing.getNodes().stream().findFirst()
        .orElseThrow(() -> new NodeNotFoundException(key.entityId()))
      )
      .getKey();
  }

  private static void handlerThrowable(final Throwable throwable, Class<?> entityClass) {
    LOGGER.error("[-- Channel for entity " + entityClass.getSimpleName() + " had to drop the following exception --]", throwable);
  }

  private static void removeHandler(final String handler) {
    final var simpleNode = SimpleNode.of(handler);
    if (actorHashRing.contains(simpleNode)) {
      LOGGER.info("Removing handler form hash ring -> " + handler);
      actorHashRing.remove(simpleNode);
    } else {
      LOGGER.info("Handler not present in hash ring -> " + handler);
    }
  }

  private static void synchronizeChannel(Message<String> objectMessage) {
    LOGGER.debug("Synchronizing handler -> " + objectMessage.body());
    switch (Actions.valueOf(objectMessage.headers().get(Actions.ACTION.name()))) {
      case ADD -> addHandler(objectMessage.body());
      case REMOVE -> removeHandler(objectMessage.body());
      default -> throw CacheException.illegalState();
    }
  }


}
