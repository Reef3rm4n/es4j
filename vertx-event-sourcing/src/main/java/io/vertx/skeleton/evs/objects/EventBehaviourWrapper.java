package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.EventBehaviour;

public record EventBehaviourWrapper<T extends EntityAggregate> (
  EventBehaviour<T, Object> delegate,
  Class<T> entityAggregateClass,
  Class<?> eventClass
){
}
