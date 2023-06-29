package io.es4j.core.objects;

import io.es4j.Aggregate;
import io.es4j.Aggregator;
import io.es4j.Event;

public record AggregatorWrap<T extends Aggregate, E extends Event> (
  Aggregator<T, E> delegate,
  Class<T> entityAggregateClass,
  Class<?> eventClass
){
}
