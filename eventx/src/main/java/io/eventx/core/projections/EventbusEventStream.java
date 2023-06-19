package io.eventx.core.projections;

import io.eventx.Aggregate;
import io.eventx.core.CommandHandler;
import io.smallrye.mutiny.Uni;
import io.eventx.EventProjection;
import io.eventx.core.objects.AggregateEvent;
import io.vertx.mutiny.core.Vertx;

import java.util.List;
import java.util.StringJoiner;

public class EventbusEventStream implements EventProjection {

  public static final String EVENT_PROJECTION = "event-projection";
  private final Vertx vertx;
  private final Class<? extends Aggregate> aggregateClass;

  public EventbusEventStream(Vertx vertx, Class<? extends Aggregate> aggregateClass) {
    this.vertx = vertx;
    this.aggregateClass = aggregateClass;
  }

  @Override
  public Uni<Void> apply(List<AggregateEvent> events) {
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

  public String eventbusAddress(AggregateEvent aggregateEvent) {
    return new StringJoiner("/")
      .add(EVENT_PROJECTION)
      .add(CommandHandler.camelToKebab(aggregateClass.getSimpleName()))
      .add(aggregateEvent.tenantId())
      .toString();
  }

  public static String eventbusAddress(Class<? extends Aggregate> aggregateClass, String tenantId) {
    return new StringJoiner("/")
      .add(EVENT_PROJECTION)
      .add(CommandHandler.camelToKebab(aggregateClass.getSimpleName()))
      .add(tenantId)
      .toString();
  }

}
