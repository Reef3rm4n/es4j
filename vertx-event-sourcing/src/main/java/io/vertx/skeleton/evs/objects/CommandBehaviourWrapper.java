package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.CommandBehaviour;
import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.Command;

public record CommandBehaviourWrapper<E extends EntityAggregate, C extends Command>(
  CommandBehaviour<E, C> delegate,
  Class<E> entityAggregateClass,
  Class<C> commandClass
) {
}

