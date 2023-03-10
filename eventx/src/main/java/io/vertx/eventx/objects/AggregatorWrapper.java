package io.vertx.eventx.objects;

import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Aggregator;
import io.vertx.eventx.Event;

public record AggregatorWrapper<T extends Aggregate, E extends Event> (
  Aggregator<T, E> delegate,
  Class<T> entityAggregateClass,
  Class<?> eventClass
){
}
