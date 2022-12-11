package io.vertx.skeleton.ccp;

import io.vertx.skeleton.ccp.models.QueueConfiguration;
import io.vertx.skeleton.models.Tenant;
import io.smallrye.mutiny.Uni;

import java.util.List;

public interface MultiProcessConsumer<T> {
  Uni<Void> process(T payload);
  default List<Tenant> tenants() {
    return null;
  }

  default QueueConfiguration configuration() {
    return null;
  }

}
