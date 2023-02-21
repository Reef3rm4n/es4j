package io.vertx.skeleton.ccp.models;

import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.skeleton.ccp.QueueMessageProcessor;
import io.vertx.skeleton.models.Tenant;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.circuitbreaker.CircuitBreaker;

import java.util.List;

public final class QueueCircuitBreaker<T> implements QueueMessageProcessor<T> {
  private final QueueMessageProcessor<T> processor;
  private final CircuitBreaker circuitBreaker;

  public QueueCircuitBreaker(QueueMessageProcessor<T> processor, CircuitBreaker circuitBreaker) {
    this.processor = processor;
    this.circuitBreaker = circuitBreaker;
  }

  @Override
  public Uni<Void> process(T payload, SqlConnection sqlConnection) {
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

  public QueueMessageProcessor<T> getDelegate() {
    return processor;
  }
}
