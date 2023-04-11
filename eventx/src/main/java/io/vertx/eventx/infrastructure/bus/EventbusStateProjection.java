package io.vertx.eventx.infrastructure.bus;

import io.smallrye.mutiny.Uni;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.StateProjection;
import io.vertx.eventx.objects.AggregateState;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.StringJoiner;


public class EventbusStateProjection<T extends Aggregate> implements StateProjection<T> {

  private final Vertx vertx;

  protected static final Logger LOGGER = LoggerFactory.getLogger(EventbusStateProjection.class);


  public EventbusStateProjection(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Uni<Void> update(AggregateState<T> currentState) {
    try {
      final var address = resolveBusAddress(currentState);
      LOGGER.debug("Publishing state change to {} {} {}", address, currentState.state().getClass().getSimpleName(), JsonObject.mapFrom(currentState).encodePrettily());
      vertx.eventBus().publish(
        address,
        JsonObject.mapFrom(currentState),
        new DeliveryOptions()
          .setLocalOnly(false)
          .addHeader("event-type", "aggregate-state")
          .addHeader("event-class", currentState.aggregateClass().getSimpleName())
      );
    } catch (Exception exception) {
      LOGGER.error("Error while running {} projection", this.getClass().getName(), exception);
    }
    return Uni.createFrom().voidItem();
  }

  private String resolveBusAddress(AggregateState<T> currentState) {
    return new StringJoiner("/")
      .add(currentState.aggregateClass().getSimpleName().toLowerCase())
      .add(currentState.state().tenantID())
      .add(currentState.state().aggregateId())
      .toString();

  }
}
