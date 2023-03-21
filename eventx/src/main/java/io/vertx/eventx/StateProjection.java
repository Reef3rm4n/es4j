package io.vertx.eventx;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.objects.AggregateState;

public interface StateProjection<T extends Aggregate> {

  Uni<Void> update(AggregateState<T> currentState);
  default String tenantID() {
    return "default";
  }


}
