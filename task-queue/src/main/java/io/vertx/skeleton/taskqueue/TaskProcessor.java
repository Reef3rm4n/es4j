package io.vertx.skeleton.taskqueue;

import io.smallrye.mutiny.Uni;
import io.vertx.skeleton.taskqueue.models.TaskTransaction;

import java.util.List;


/**
 * When using mono consumer the queue entry will be processed only ONCE by either the default implementation
 * which is the one that returns tenant null or the tenant specific implementation which is the implementation that
 * returns a matching tenant in the tenants() method
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
