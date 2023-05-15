package io.vertx.eventx.core;

import io.smallrye.mutiny.Uni;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.StateProjection;
import io.vertx.eventx.objects.AggregateState;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.StringJoiner;


public class EventbusStateProjection<T extends Aggregate> implements StateProjection<T> {

  private final Vertx vertx;

  private final Logger LOGGER = LoggerFactory.getLogger(EventbusStateProjection.class);


  public EventbusStateProjection(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Uni<Void> update(AggregateState<T> currentState) {
    final var address = subscriptionAddress(currentState.state().getClass());
    try {
      vertx.eventBus().publish(
        subscriptionAddress(currentState.state().getClass()),
        currentState.toJson()
      );
    } catch (Exception exception) {
      LOGGER.error("Unable to publish state update for {}::{} on address {}", currentState.aggregateClass().getSimpleName(), currentState.state().aggregateId(), address);
      return Uni.createFrom().failure(exception);
    }
    LOGGER.debug("State update published for {}::{} to address {}", currentState.aggregateClass().getSimpleName(), currentState.state().aggregateId(), address);
    return Uni.createFrom().voidItem();
  }

  public static String subscriptionAddress(Class<? extends Aggregate> aggregateClass) {
    return new StringJoiner("/")
      .add("state-projection")
      .add(aggregateClass.getSimpleName().toLowerCase())
      .toString();

  }


}
