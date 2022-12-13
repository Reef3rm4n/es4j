package io.vertx.skeleton.ccp;

import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.skeleton.ccp.models.QueueConfiguration;
import io.vertx.skeleton.models.Tenant;
import io.smallrye.mutiny.Uni;

import java.util.List;


/**
 * When using mono consumer the queue entry will be processed only ONCE by either the default implementation
 * which is the one that returns tenant null or the tenant specific implementation which is the implementation that
 * returns a matching tenant in the tenants() method
 * @param <T> The payload, queue entry type
 * @param <R> The result, consumer can produce a result that will be passed to event consumers
 */
public interface SingleProcessConsumer<T, R> {
  Uni<R> process(T payload, SqlConnection sqlConnection);
  default List<Class<Throwable>> fatal() {
    return List.of();
  }

  default List<Tenant> tenants() {
    return null;
  }

  default QueueConfiguration configuration() {
    return null;
  }

}
