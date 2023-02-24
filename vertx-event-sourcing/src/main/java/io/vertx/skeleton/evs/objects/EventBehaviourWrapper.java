package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.Entity;
import io.vertx.skeleton.evs.Aggregator;

public record EventBehaviourWrapper<T extends Entity> (
  Aggregator<T, Object> delegate,
  Class<T> entityAggregateClass,
  Class<?> eventClass
){
}
