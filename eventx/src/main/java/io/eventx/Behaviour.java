package io.eventx;

import java.util.Collections;
import java.util.List;

public interface Behaviour<T extends Aggregate, C extends Command> {

  default List<String> requiredRoles() {
    return Collections.emptyList();
  }

  List<Event> process(T state, C command);

  default String tenantID() {
    return "default";
  }
}
