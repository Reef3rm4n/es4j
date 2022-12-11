package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.EventSourcingHttpRoute;

public record VertxEventSourcingRouteWrapper<T extends EntityAggregate,R extends EventSourcingHttpRoute<T>>(
  R route,
  Class<T> entityAggregateClass
) {
}
