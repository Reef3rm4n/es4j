package io.vertx.eventx.core;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.EventProjection;
import io.vertx.eventx.objects.PolledEvent;
import io.vertx.mutiny.core.Vertx;

import java.util.List;
import java.util.StringJoiner;

public class EventbusEventProjection implements EventProjection {

  private final Vertx vertx;
  private final Class<? extends Aggregate> aggregateClass;

  public EventbusEventProjection(Vertx vertx, Class<? extends Aggregate> aggregateClass) {
    this.vertx = vertx;
    this.aggregateClass = aggregateClass;
  }

  @Override
  public Uni<Void> apply(List<PolledEvent> events) {
    try {
      events.forEach(polledEvent -> vertx.eventBus().publish(
          eventbusAddress(),
          polledEvent.toJson()
        )
      );
    } catch (Exception exception) {
      return Uni.createFrom().failure(exception);
    }
    return Uni.createFrom().voidItem();
  }

  public String eventbusAddress() {
    return new StringJoiner("/")
      .add("event-projection")
      .add(aggregateClass.getSimpleName())
      .toString();
  }

  public static String eventbusAddress(Class<? extends Aggregate> aggregateClass) {
    return new StringJoiner("/")
      .add("event-projection")
      .add(aggregateClass.getSimpleName())
      .toString();
  }

}
