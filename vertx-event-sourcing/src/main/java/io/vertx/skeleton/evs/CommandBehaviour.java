package io.vertx.skeleton.evs;

import java.util.List;

public interface CommandBehaviour<T extends EntityAggregate, C extends Command> {

  List<Object> process(T state, C command);

  default String tenantID() {
    return "default";
  }
}
