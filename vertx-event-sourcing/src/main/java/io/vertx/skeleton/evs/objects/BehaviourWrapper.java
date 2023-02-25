package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.Behaviour;
import io.vertx.skeleton.evs.Entity;
import io.vertx.skeleton.evs.Command;

public record BehaviourWrapper<E extends Entity, C extends Command>(
  Behaviour<E, C> delegate,
  Class<E> entityAggregateClass,
  Class<C> commandClass
) {
}

