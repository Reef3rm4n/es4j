package io.eventx.core.objects;

import io.eventx.Aggregate;
import io.eventx.Aggregator;
import io.eventx.Event;

public record AggregatorWrap<T extends Aggregate, E extends Event> (
  Aggregator<T, E> delegate,
  Class<T> entityAggregateClass,
  Class<?> eventClass
){
}
