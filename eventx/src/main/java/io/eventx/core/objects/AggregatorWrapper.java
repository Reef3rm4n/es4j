package io.eventx.core.objects;

import io.eventx.Aggregate;
import io.eventx.EventBehaviour;
import io.eventx.Event;

public record AggregatorWrapper<T extends Aggregate, E extends Event> (
  EventBehaviour<T, E> delegate,
  Class<T> entityAggregateClass,
  Class<?> eventClass
){
}
