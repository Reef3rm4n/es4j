package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.Projection;

public record ProjectionWrapper<T extends EntityAggregate>(
  Projection<T> delegate,
  Class<T> entityAggregateClass
) {
}
