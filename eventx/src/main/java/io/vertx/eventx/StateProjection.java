package io.vertx.eventx;

import io.smallrye.mutiny.Uni;

public interface StateProjection<T> {

  Uni<Void> update(T currentState);
  default String tenantID() {
    return "default";
  }


}
