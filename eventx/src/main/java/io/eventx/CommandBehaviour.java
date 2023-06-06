package io.eventx;

import java.util.Collections;
import java.util.List;

public interface CommandBehaviour<T extends Aggregate, C extends Command> {
  List<Event> process(T state, C command);
  default String tenantID() {
    return "default";
  }
}
