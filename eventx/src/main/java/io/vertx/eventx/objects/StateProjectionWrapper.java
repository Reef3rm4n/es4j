package io.vertx.eventx.objects;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.StateProjection;

public record StateProjectionWrapper<T extends Aggregate>(
  StateProjection<T> stateProjection,
  Class<T> entityAggregateClass
)  {
  public  Uni<Void> update(Object state) {
    return uniVoid(state);
  }

  private Uni<Void> uniVoid(Object state) {
    return stateProjection.update(entityAggregateClass.cast(state));
  }
}
