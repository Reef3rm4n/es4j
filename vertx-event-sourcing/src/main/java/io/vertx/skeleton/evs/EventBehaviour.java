package io.vertx.skeleton.evs;

import io.vertx.skeleton.models.Tenant;

public interface EventBehaviour<T extends EntityAggregate, E> {

  T apply(T aggregateState, E event);

  default Tenant tenant() {
    return null;
  }

}
