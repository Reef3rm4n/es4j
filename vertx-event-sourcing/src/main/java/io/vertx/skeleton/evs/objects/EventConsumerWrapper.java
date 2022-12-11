package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.EventConsumer;

public record EventConsumerWrapper<T extends EntityAggregate>(
  EventConsumer<T> delegate,
  Class<T> entityAggregateClass
) {
}
