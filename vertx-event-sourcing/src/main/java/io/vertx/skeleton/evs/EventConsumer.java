package io.vertx.skeleton.evs;

import io.vertx.skeleton.models.Tenant;

import java.util.List;

public interface EventConsumer<T extends EntityAggregate> {


  void consumeEvent(String entityId, Tenant tenant, Object event);
  Class<T> entityAggregateClass();

  default List<Class<?>> events() {
    return null;
  }

}
