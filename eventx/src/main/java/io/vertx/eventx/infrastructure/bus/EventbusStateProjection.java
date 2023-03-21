package io.vertx.eventx.infrastructure.bus;

import io.smallrye.mutiny.Uni;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.StateProjection;
import io.vertx.eventx.objects.AggregateState;
import io.vertx.mutiny.core.Vertx;

import java.util.StringJoiner;


public class EventbusStateProjection<T extends Aggregate> implements StateProjection<T> {

  private final Vertx vertx;

  public EventbusStateProjection(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Uni<Void> update(AggregateState<T> currentState) {
    try {
      vertx.eventBus().publish(
        resolveBusAddress(currentState),
        JsonObject.mapFrom(currentState),
        new DeliveryOptions()
          .setLocalOnly(false)
          .addHeader("event-type", "aggregate-state")
          .addHeader("event-class", currentState.aggregateClass().getSimpleName())
      );
    } catch (Exception exception) {
      return Uni.createFrom().failure(exception);
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
