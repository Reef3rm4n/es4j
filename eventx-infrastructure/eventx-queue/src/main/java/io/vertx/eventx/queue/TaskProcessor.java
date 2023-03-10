package io.vertx.eventx.queue;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.queue.models.TaskTransaction;

import java.util.List;


/**
 * When using mono consumer the queue entry will be processed only ONCE by either the default implementation
 * which is the one that returns tenantId null or the tenantId specific implementation which is the implementation that
 * returns a matching tenantId in the tenants() method
 *
 * @param <T> The payload, queue entry type
 */
public interface TaskProcessor<T> {
  Uni<Void> process(T payload, TaskTransaction taskTransaction);

  default List<Class<Throwable>> fatalExceptions() {
    return List.of();
  }

  default List<String> tenants() {
    return null;
  }

  default Boolean blockingProcessor() {
    return Boolean.FALSE;
  }

}
