package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.CommandValidator;
import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.EntityAggregateCommand;

public record CommandValidatorWrapper<E extends EntityAggregate,C extends EntityAggregateCommand>(
  CommandValidator<E,C> delegate,
  Class<E> entityAggregateClass,
  Class<C> commandClass
) {
}
