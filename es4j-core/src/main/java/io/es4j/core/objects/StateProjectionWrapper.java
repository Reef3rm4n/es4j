package io.es4j.core.objects;

import io.es4j.Aggregate;
import io.es4j.AsyncStateTransfer;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;

public record StateProjectionWrapper<T extends Aggregate>(
  AsyncStateTransfer<T> asyncStateTransfer,
  Class<T> entityAggregateClass,
  Logger logger
) {
  public Uni<Void> update(AggregateState<T> state) {
    return asyncStateTransfer.update(state);
  }
}
