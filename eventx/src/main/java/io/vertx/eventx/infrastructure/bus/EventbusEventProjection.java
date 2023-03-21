package io.vertx.eventx.infrastructure.bus;

import io.smallrye.mutiny.Uni;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.EventProjection;
import io.vertx.eventx.objects.PolledEvent;
import io.vertx.mutiny.core.Vertx;

import java.util.Comparator;
import java.util.List;
import java.util.StringJoiner;

public class EventbusEventProjection implements EventProjection {

  private final Vertx vertx;

  public EventbusEventProjection(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Uni<Void> apply(List<PolledEvent> events) {
    try {
      events.stream().sorted(Comparator.comparingLong(PolledEvent::journalOffset))
        .forEach(polledEvent -> vertx.eventBus().publish(
            parseEventbusAddress(polledEvent),
            JsonObject.mapFrom(polledEvent),
            new DeliveryOptions()
              .setLocalOnly(false)
              .addHeader("event-type", "aggregate-event")
              .addHeader("event-class", polledEvent.event().getClass().getSimpleName())
          )
        );
    } catch (Exception exception) {
      return Uni.createFrom().failure(exception);
    }
    return Uni.createFrom().voidItem();
  }

  private String parseEventbusAddress(PolledEvent polledEvent) {
    return new StringJoiner("/")
      .add(polledEvent.aggregateClass())
      .add(polledEvent.tenantId())
      .add(polledEvent.aggregateId())
      .toString();
  }
}
