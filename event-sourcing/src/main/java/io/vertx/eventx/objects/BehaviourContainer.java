package io.vertx.eventx.objects;

import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Command;
import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.exceptions.UnknownCommand;

import java.util.List;
import java.util.Map;

public record BehaviourContainer<T extends Aggregate>(
  Map<Class<? extends Command>, BehaviourWrapper<T, ? extends Command>> behaviours
) {



  public List<Object> process(T state, Command  command) {
    final var behaviour = behaviours.get(command.getClass());
    if (behaviour == null) {
      throw new UnknownCommand(new EventXError("","",999));
    }
    return behaviour.process(state, command);
  }

}
