package io.vertx.eventx.infrastructure.bus;

import io.smallrye.mutiny.Uni;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.EventProjection;
import io.vertx.eventx.objects.PolledEvent;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.StringJoiner;

public class EventbusEventProjection implements EventProjection {

  private final Vertx vertx;
  protected static final Logger LOGGER = LoggerFactory.getLogger(EventbusEventProjection.class);

  public EventbusEventProjection(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Uni<Void> apply(List<PolledEvent> events) {
    events.stream().sorted(Comparator.comparingLong(PolledEvent::journalOffset))
      .forEach(polledEvent -> {
          try {
            final var address = parseEventbusAddress(polledEvent);
            LOGGER.debug("Publishing {} {}", polledEvent.event().getClass().getSimpleName(), JsonObject.mapFrom(polledEvent).encodePrettily());
            vertx.eventBus().publish(
              address,
              JsonObject.mapFrom(polledEvent),
              new DeliveryOptions()
                .setLocalOnly(false)
                .addHeader("event-type", "aggregate-event")
                .addHeader("event-class", polledEvent.event().getClass().getSimpleName())
            );
          } catch (Exception e) {
            LOGGER.error("Error publishing {}", polledEvent, e);
          }
        }
      );
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
