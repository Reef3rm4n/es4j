package io.vertx.eventx.objects;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.StateProjection;
import org.slf4j.Logger;

public record StateProjectionWrapper<T extends Aggregate>(
  StateProjection<T> stateProjection,
  Class<T> entityAggregateClass,
  Logger logger
)  {
  public  Uni<Void> update(AggregateState<T> state) {
    return stateProjection.update(state);
  }
}
