package io.eventx.queue;

import io.eventx.queue.models.QueueTransaction;
import io.smallrye.mutiny.Uni;

import java.util.List;


/**
 * When using mono consumer the queue entry will be processed only ONCE by either the default implementation
 * which is the one that returns tenantId null or the tenantId specific implementation which is the implementation that
 * returns a matching tenantId in the tenants() method
 *
 * @param <T> The payload, queue entry type
 */
public interface MessageProcessor<T> {
  Uni<Void> process(T payload, QueueTransaction queueTransaction);

  default List<Class<? extends Throwable>> fatalExceptions() {
    return List.of();
  }

  default List<String> tenants() {
    return null;
  }

  default Boolean blockingProcessor() {
    return Boolean.FALSE;
  }

}
