package io.vertx.skeleton.ccp;


import io.vertx.skeleton.ccp.models.QueueEvent;
import io.vertx.skeleton.models.Tenant;
import io.smallrye.mutiny.Uni;

import java.util.List;


public interface ProcessEventConsumer<T, R> {

  Uni<Void> consume(List<QueueEvent<T, R>> results);

  default List<Tenant> tenants() {
    return null;
  }

}
