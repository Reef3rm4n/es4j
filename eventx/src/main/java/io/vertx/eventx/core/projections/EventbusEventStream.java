package io.vertx.eventx.core.projections;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.EventProjection;
import io.vertx.eventx.core.objects.PolledEvent;
import io.vertx.mutiny.core.Vertx;

import java.util.List;
import java.util.StringJoiner;

import static io.vertx.eventx.core.CommandHandler.camelToKebab;

public class EventbusEventStream implements EventProjection {

  public static final String EVENT_PROJECTION = "event-projection";
  private final Vertx vertx;
  private final Class<? extends Aggregate> aggregateClass;

  public EventbusEventStream(Vertx vertx, Class<? extends Aggregate> aggregateClass) {
    this.vertx = vertx;
    this.aggregateClass = aggregateClass;
  }

  @Override
  public Uni<Void> apply(List<PolledEvent> events) {
    try {
      events.forEach(polledEvent -> vertx.eventBus().publish(
          eventbusAddress(polledEvent),
          polledEvent.toJson()
        )
      );
    } catch (Exception exception) {
      return Uni.createFrom().failure(exception);
    }
    return Uni.createFrom().voidItem();
  }

  public String eventbusAddress(PolledEvent polledEvent) {
    return new StringJoiner("/")
      .add(EVENT_PROJECTION)
      .add(camelToKebab(aggregateClass.getSimpleName()))
      .add(polledEvent.tenantId())
      .toString();
  }

  public static String eventbusAddress(Class<? extends Aggregate> aggregateClass, String tenantId) {
    return new StringJoiner("/")
      .add(EVENT_PROJECTION)
      .add(camelToKebab(aggregateClass.getSimpleName()))
      .add(tenantId)
      .toString();
  }

}
