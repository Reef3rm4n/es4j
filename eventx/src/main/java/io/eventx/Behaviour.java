package io.eventx;

import java.util.List;

public interface Behaviour<T extends Aggregate, C extends Command> {
  List<Event> process(T state, C command);
  default String tenant() {
    return "default";
  }
}
