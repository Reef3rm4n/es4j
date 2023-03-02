package io.vertx.eventx;

import io.smallrye.mutiny.Uni;

import java.util.List;

public interface Projection<T> {

  Uni<Void> update(T currentState, List<Object> events);

  default List<Class<?>> events() {
    return null;
  }

  default String tenantID() {
    return "default";
  }


}
