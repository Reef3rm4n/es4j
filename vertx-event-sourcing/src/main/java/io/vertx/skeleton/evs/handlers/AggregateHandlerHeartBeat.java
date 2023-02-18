package io.vertx.skeleton.evs.handlers;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.mutiny.core.Vertx;
import io.vertx.skeleton.evs.cache.Actions;
import io.vertx.skeleton.evs.cache.AddressResolver;

public class AggregateHandlerHeartBeat<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateHandlerHeartBeat.class);
  private final Vertx vertx;
  private final Integer heartBeatInterval;
  private final Class<T> aggregateClass;
  private final String handlerAddress;
  private long refreshTaskTimerId;

  public AggregateHandlerHeartBeat(
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
    LOGGER.info("Publishing handler registration -> " + handlerAddress);
    vertx.eventBus().publish(AddressResolver.localAvailableHandlers(aggregateClass), handlerAddress, new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.ADD.name()));
    // publishes heart beats for aggregate handlers.
    handlerRefreshTimer(heartBeatInterval);
  }

  private void handlerRefreshTimer(Integer delay) {
    this.refreshTaskTimerId = vertx.setTimer(
      delay,
      d -> {
        vertx.eventBus().publish(AddressResolver.localAvailableHandlers(aggregateClass), handlerAddress, new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.ADD.name()));
        LOGGER.info("Published handler hearth beat -> " + handlerAddress);
        handlerRefreshTimer(delay);
      }
    );
  }

  public void unregisterHandler() {
    LOGGER.info("Unregistering handler for " + aggregateClass + " -> " + handlerAddress);
    vertx.eventBus().publish(AddressResolver.localAvailableHandlers(aggregateClass), handlerAddress, new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.REMOVE.name()));
    vertx.cancelTimer(refreshTaskTimerId);
  }

}
