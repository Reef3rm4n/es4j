package io.eventx.core.objects;

import io.eventx.CommandBehaviour;
import io.eventx.Aggregate;
import io.eventx.Command;
import io.eventx.Event;

import java.util.List;

public record BehaviourWrapper<A extends Aggregate, C extends Command>(
  CommandBehaviour<A, C> delegate,
  Class<A> entityAggregateClass,
  Class<C> commandClass
) {


  public List<Event> process(A state, Command command) {
    return delegate.process(state, commandClass.cast(command));
  }


}

