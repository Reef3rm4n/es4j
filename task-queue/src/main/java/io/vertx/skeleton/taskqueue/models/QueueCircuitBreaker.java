package io.vertx.skeleton.taskqueue.models;

import io.vertx.skeleton.taskqueue.TaskProcessor;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.circuitbreaker.CircuitBreaker;

import java.util.List;

public final class QueueCircuitBreaker<T> implements TaskProcessor<T> {
  private final TaskProcessor<T> processor;
  private final CircuitBreaker circuitBreaker;

  public QueueCircuitBreaker(TaskProcessor<T> processor, CircuitBreaker circuitBreaker) {
    this.processor = processor;
    this.circuitBreaker = circuitBreaker;
  }

  @Override
  public Uni<Void> process(T payload, TaskTransaction sqlConnection) {
    return circuitBreaker.execute(processor.process(payload, sqlConnection));
  }

  @Override
  public List<Class<Throwable>> fatalExceptions() {
    return processor.fatalExceptions();
  }

  @Override
  public List<String> tenants() {
    return processor.tenants();
  }

  public TaskProcessor<T> getDelegate() {
    return processor;
  }
}
