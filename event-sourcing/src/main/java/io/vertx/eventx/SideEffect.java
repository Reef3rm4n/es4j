package io.vertx.eventx;

import io.smallrye.mutiny.Uni;

public interface SideEffect<T extends Aggregate, C extends Command> {

  Uni<Void> perform(T aggregateState, C command);

  default String tenantID() {
    return "default";
  }


}
