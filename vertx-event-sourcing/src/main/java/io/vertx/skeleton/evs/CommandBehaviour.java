package io.vertx.skeleton.evs;

import io.vertx.skeleton.models.Tenant;

import java.util.List;

public interface CommandBehaviour<T extends EntityAggregate, C extends EntityAggregateCommand> {

  List<Object> process(T state, C command);

  default Tenant tenant() {
    return null;
  }
}
