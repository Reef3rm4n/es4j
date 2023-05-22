package io.eventx.core.objects;

import io.eventx.Aggregate;
import io.eventx.StateProjection;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;

public record StateProjectionWrapper<T extends Aggregate>(
  StateProjection<T> stateProjection,
  Class<T> entityAggregateClass,
  Logger logger
) {
  public Uni<Void> update(AggregateState<T> state) {
    return stateProjection.update(state);
  }
}
