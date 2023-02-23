package io.vertx.skeleton.evs;

public interface EventBehaviour<T extends EntityAggregate, E> {

  T apply(T aggregateState, E event);


  default String tenantID() {
    return "default";
  }

}
