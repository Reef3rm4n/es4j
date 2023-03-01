package io.vertx.eventx;

import io.smallrye.mutiny.Uni;

import java.util.List;

public interface SideEffect<T extends Aggregate> {
  // todo this will be triggered after a command
  Uni<Void> perform(T aggregateState);
  List<Class<? extends Command>> commands();

  default String tenantID() {
    return "default";
  }

}
