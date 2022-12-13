package io.vertx.skeleton.ccp.models;

import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.skeleton.ccp.SingleProcessConsumer;
import io.vertx.skeleton.models.Tenant;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.circuitbreaker.CircuitBreaker;

import java.util.List;

public final class SingleWithCircuitBreaker<T, R> implements SingleProcessConsumer<T, R> {
  private final SingleProcessConsumer<T, R> processor;
  private final CircuitBreaker circuitBreaker;

  public SingleWithCircuitBreaker(SingleProcessConsumer<T, R> processor, CircuitBreaker circuitBreaker) {
    this.processor = processor;
    this.circuitBreaker = circuitBreaker;
  }

  @Override
  public Uni<R> process(T payload, SqlConnection sqlConnection) {
    return circuitBreaker.execute(processor.process(payload, sqlConnection));
  }

  @Override
  public List<Class<Throwable>> fatal() {
    return processor.fatal();
  }

  @Override
  public List<Tenant> tenants() {
    return processor.tenants();
  }

  @Override
  public QueueConfiguration configuration() {
    return processor.configuration();
  }

  public SingleProcessConsumer<T,R> getDelegate() {
    return processor;
  }
}
