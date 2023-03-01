package io.vertx.eventx.objects;

import io.vertx.eventx.Behaviour;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Command;

public record BehaviourWrapper<E extends Aggregate, C extends Command>(
  Behaviour<E, C> delegate,
  Class<E> entityAggregateClass,
  Class<C> commandClass
) {
}

