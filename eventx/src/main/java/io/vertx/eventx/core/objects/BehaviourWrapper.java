package io.vertx.eventx.core.objects;

import io.vertx.eventx.Behaviour;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Command;
import io.vertx.eventx.Event;

import java.util.List;

public record BehaviourWrapper<A extends Aggregate, C extends Command>(
  Behaviour<A, C> delegate,
  Class<A> entityAggregateClass,
  Class<C> commandClass
) {


  public List<Event> process(A state, Command command) {
    return delegate.process(state, commandClass.cast(command));
  }


}

