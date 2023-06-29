package io.eventx.infrastructure.proxy;

import io.eventx.Aggregate;
import io.eventx.Command;
import io.eventx.core.objects.*;
import io.eventx.infrastructure.bus.AggregateBus;
import io.eventx.infrastructure.bus.ProjectionService;
import io.eventx.infrastructure.models.Event;
import io.eventx.infrastructure.models.ProjectionStream;
import io.eventx.infrastructure.models.ResetProjection;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.mutiny.core.Vertx;

import java.util.List;


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

  public Uni<List<Event>> projectionNext(ProjectionStream projectionStream) {
    return vertx.eventBus().<JsonArray>request(ProjectionService.nextAddress(aggregateClass), JsonObject.mapFrom(projectionStream))
      .map(jsonArrayMessage -> jsonArrayMessage.body().stream().map(JsonObject::mapFrom).map(jsonObject -> jsonObject.mapTo(Event.class)).toList());
  }
  public Uni<Void> resetProjection(ResetProjection resetProjection) {
    return vertx.eventBus().<JsonArray>request(ProjectionService.resetAddress(aggregateClass),
        new JsonObject()
          .put("tenant", resetProjection.tenantId())
          .put("projectionId", resetProjection.projectionId())
          .put("idOffset", resetProjection.idOffset())
      )
      .replaceWithVoid();
  }

}
