package io.eventx.core.objects;

import io.eventx.Aggregate;
import io.eventx.PollingStateProjection;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;

public record StateProjectionWrapper<T extends Aggregate>(
  PollingStateProjection<T> pollingStateProjection,
  Class<T> entityAggregateClass,
  Logger logger
) {
  public Uni<Void> update(AggregateState<T> state) {
    return pollingStateProjection.update(state);
  }
}
