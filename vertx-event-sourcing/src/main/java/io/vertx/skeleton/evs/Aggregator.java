package io.vertx.skeleton.evs;

public interface Aggregator<T extends Entity, E> {

  T apply(T aggregateState, E event);


  default String tenantId() {
    return "default";
  }

}
