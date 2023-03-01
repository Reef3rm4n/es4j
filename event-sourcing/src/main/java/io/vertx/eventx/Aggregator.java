package io.vertx.eventx;

public interface Aggregator<T extends Aggregate, E> {

  T apply(T aggregateState, E event);


  default String tenantId() {
    return "default";
  }

}
