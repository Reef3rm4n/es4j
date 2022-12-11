package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.ccp.MultiProcessConsumer;
import io.vertx.skeleton.models.Tenant;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.circuitbreaker.CircuitBreaker;

import java.util.List;

public record MultiWithCircuitBreaker<T>(
  MultiProcessConsumer<T> processor,
  CircuitBreaker circuitBreaker
) implements MultiProcessConsumer<T> {

  @Override
  public Uni<Void> process(T payload) {
    return circuitBreaker.execute(processor.process(payload));
  }

  @Override
  public List<Tenant> tenants() {
    return processor.tenants();
  }

  @Override
  public QueueConfiguration configuration() {
    return processor.configuration();
  }

  public MultiProcessConsumer<T> getDelegate() {
    return processor;
  }
}
