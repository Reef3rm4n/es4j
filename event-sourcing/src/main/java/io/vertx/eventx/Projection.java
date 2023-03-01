package io.vertx.eventx;

import io.smallrye.mutiny.Uni;

import java.util.List;

public interface Projection<T> {

  Uni<Void> update(T state, List<Object> eventClasses);

  default List<Class<?>> eventClasses() {
    return null;
  }

  default String tenantID() {
    return "default";
  }


}
