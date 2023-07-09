package io.es4j.infrastructure.proxy;

import io.es4j.Aggregate;
import io.es4j.Command;
import io.es4j.core.objects.*;
import io.es4j.infrastructure.bus.AggregateBus;
import io.es4j.infrastructure.bus.Es4jService;
import io.es4j.infrastructure.models.Event;
import io.es4j.infrastructure.models.EventFilter;
import io.es4j.infrastructure.models.FetchNextEvents;
import io.es4j.infrastructure.models.ResetProjection;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.mutiny.core.Vertx;

import java.util.List;
import java.util.Objects;


public class AggregateEventBusPoxy<T extends Aggregate> {
  private final Vertx vertx;
  public final Class<T> aggregateClass;
  public static final Logger LOGGER = LoggerFactory.getLogger(AggregateEventBusPoxy.class);

  public AggregateEventBusPoxy(
    final Vertx vertx,
    final Class<T> aggregateClass
  ) {
    this.vertx = vertx;
    this.aggregateClass = aggregateClass;
  }


  public Uni<AggregateState<T>> proxyCommand(Command command) {
    return AggregateBus.request(
      vertx,
      aggregateClass,
      command
    );
  }

  public Uni<AggregateState<T>> proxyCommand(Command command, List<String> roles) {
    return AggregateBus.request(
      vertx,
      aggregateClass,
      command
    );
  }

  public Uni<List<Event>> fetch(EventFilter eventStreamQuery) {
    return vertx.eventBus().<JsonArray>request(Es4jService.fetchEventsAddress(aggregateClass), JsonObject.mapFrom(eventStreamQuery))
      .map(jsonArrayMessage -> jsonArrayMessage.body().stream().map(JsonObject::mapFrom).map(jsonObject -> jsonObject.mapTo(Event.class)).toList());
  }

  public Uni<List<Event>> projectionNext(FetchNextEvents eventStreamQuery) {
    Objects.requireNonNull(eventStreamQuery.projectionId());
    return vertx.eventBus().<JsonArray>request(Es4jService.fetchNextEventsAddress(aggregateClass), JsonObject.mapFrom(eventStreamQuery))
      .map(jsonArrayMessage -> jsonArrayMessage.body().stream().map(JsonObject::mapFrom).map(jsonObject -> jsonObject.mapTo(Event.class)).toList());
  }

  public Uni<Void> resetProjection(ResetProjection resetProjection) {
    return vertx.eventBus().<JsonArray>request(Es4jService.resetOffsetAddress(aggregateClass),
        new JsonObject()
          .put("tenant", resetProjection.tenantId())
          .put("projectionId", resetProjection.projectionId())
          .put("idOffset", resetProjection.idOffset())
      )
      .replaceWithVoid();
  }

}
