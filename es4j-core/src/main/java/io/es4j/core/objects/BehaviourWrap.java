package io.es4j.core.objects;

import io.es4j.Behaviour;
import io.es4j.Aggregate;
import io.es4j.Command;
import io.es4j.Event;

import java.util.List;

public record BehaviourWrap<A extends Aggregate, C extends Command>(
  Behaviour<A, C> delegate,
  Class<A> entityAggregateClass,
  Class<C> commandClass
) {


  public List<Event> process(A state, Command command) {
    return delegate.process(state, commandClass.cast(command));
  }


}

