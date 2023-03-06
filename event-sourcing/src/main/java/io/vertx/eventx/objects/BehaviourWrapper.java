package io.vertx.eventx.objects;

import io.vertx.eventx.Behaviour;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Command;

import java.util.List;

public record BehaviourWrapper<E extends Aggregate, C extends Command>(
  Behaviour<E, C> delegate,
  Class<E> entityAggregateClass,
  Class<C> commandClass
) {


  public List<Object> process(E state, Command command) {
    return delegate.process(state, commandClass.cast(command));
  }


}

