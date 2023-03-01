package io.vertx.eventx.objects;

import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Aggregator;

public record AggregatorWrapper<T extends Aggregate> (
  Aggregator<T, Object> delegate,
  Class<T> entityAggregateClass,
  Class<?> eventClass
){
}
