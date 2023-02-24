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
import io.vertx.skeleton.evs.cache.Actions;
import io.vertx.skeleton.evs.cache.AddressResolver;
import io.vertx.skeleton.evs.exceptions.NodeNotFoundException;
import io.vertx.skeleton.evs.objects.EntityAggregateKey;
import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.CacheException;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.time.Duration;

public class Channel<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(Channel.class);
  private final Vertx vertx;
  private final Integer heartBeatInterval;
  private final Class<T> aggregateClass;
  private final String handlerAddress;
  private long refreshTaskTimerId;

  public Channel(
    final Vertx vertx,
    final Integer heartBeatInterval,
    final Class<T> aggregateClass,
    final String handlerAddress
  ) {
    this.vertx = vertx;
    this.heartBeatInterval = heartBeatInterval;
    this.aggregateClass = aggregateClass;
    this.handlerAddress = handlerAddress;
  }


  public void registerHandler() {
    LOGGER.info("Publishing handler registration [address: " + handlerAddress + "]");
    vertx.eventBus().<String>publish(
      AddressResolver.actorChannel(aggregateClass),
      handlerAddress,
      new DeliveryOptions()
        .setTracingPolicy(TracingPolicy.ALWAYS)
        .addHeader(Actions.ACTION.name(), Actions.ADD.name())
    );
    // publishes heart beats for aggregate handlers.
    handlerRefreshTimer(heartBeatInterval);
  }

  private void handlerRefreshTimer(Integer delay) {
    this.refreshTaskTimerId = vertx.setTimer(
      delay,
      d -> {
        vertx.eventBus().publish(
          AddressResolver.actorChannel(aggregateClass),
          handlerAddress,
          new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.ADD.name())
        );
        LOGGER.info("Handler hearth beat [address: " + handlerAddress + "]");
        handlerRefreshTimer(delay);
      }
    );
  }

  public void unregisterHandler() {
    LOGGER.info("Unregistering handler for [ " + aggregateClass + "] [address: " + handlerAddress + "]");
    vertx.eventBus().publish(
      AddressResolver.actorChannel(aggregateClass),
      handlerAddress,
      new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.REMOVE.name())
    );
    vertx.cancelTimer(refreshTaskTimerId);
  }

  public void pollEntityActors() {
    vertx.eventBus().publish(
      AddressResolver.actorChannel(aggregateClass),
      handlerAddress,
      new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.REMOVE.name())
    );
  }
  private void addHandler(final String handler) {
    final var simpleNode = SimpleNode.of(handler);
    if (!hashRing.contains(simpleNode)) {
      LOGGER.info("Adding new handler to hash-ring -> " + handler);
      hashRing.add(simpleNode);
    } else {
      LOGGER.info("Handler already present in hash-ring -> " + handler);
    }
  }

  public String locateHandler(EntityAggregateKey key) {
    LOGGER.info("Looking for handler for entity -> " + key);
    final var node = hashRing.locate(key.entityId());
    LOGGER.info("Resolved to node -> " + node);
    return node.orElse(hashRing.getNodes().stream().findFirst()
        .orElseThrow(() -> new NodeNotFoundException(key.entityId()))
      )
      .getKey();
  }

  public Uni<ChannelProxy<T>> start() {
    // this is responsible for the synchronization of the hash-ring when nodes leave the cluster...
    return vertx.eventBus().<String>consumer(AddressResolver.actorChannel(aggregateEntityClass))
      .handler(this::synchronizeHashRing)
      .exceptionHandler(this::handlerThrowable)
      .completionHandler()
      .flatMap(avoid -> Multi.createBy().repeating().supplier(() -> hashRing.getNodes().isEmpty())
        .atMost(10).capDemandsTo(1).paceDemand()
        .using(new FixedDemandPacer(1, Duration.ofMillis(500)))
        .collect().last()
        .map(Unchecked.function(
            aBoolean -> {
              if (Boolean.TRUE.equals(aBoolean)) {
                throw new NodeNotFoundException(new Error("Hash ring empty", "Hash ring synchronizer was still empty after 10 seconds, there's no entity deployed in the platform", -999));
              }
              return this;
            }
          )
        )
      );
  }

  private void synchronizeHashRing(Message<String> objectMessage) {
    LOGGER.debug("Synchronizing handler -> " + objectMessage.body());
    switch (Actions.valueOf(objectMessage.headers().get(Actions.ACTION.name()))) {
      case ADD -> addHandler(objectMessage.body());
      case REMOVE -> removeHandler(objectMessage.body());
      default -> throw CacheException.illegalState();
    }
  }

  private void removeHandler(final String handler) {
    final var simpleNode = SimpleNode.of(handler);
    if (hashRing.contains(simpleNode)) {
      LOGGER.info("Removing handler form hash ring -> " + handler);
      hashRing.remove(simpleNode);
    } else {
      LOGGER.info("Handler not present in hash ring -> " + handler);
    }
  }

  private void handlerThrowable(final Throwable throwable) {
    LOGGER.error("[-- EntityAggregateSynchronizer for entity " + aggregateEntityClass.getSimpleName() + " had to drop the following exception --]", throwable);
  }



}
