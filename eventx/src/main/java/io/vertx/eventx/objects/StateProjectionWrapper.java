package io.vertx.eventx.objects;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.StateProjection;

public record StateProjectionWrapper<T extends Aggregate>(
  StateProjection<T> stateProjection,
  Class<T> entityAggregateClass
)  {
  public  Uni<Void> update(AggregateState<T> state) {
    return stateProjection.update(state);
  }
}
