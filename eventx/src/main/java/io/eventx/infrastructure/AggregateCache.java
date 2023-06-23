package io.eventx.infrastructure;

import io.eventx.Aggregate;
import io.eventx.core.objects.AggregateState;
import io.eventx.infrastructure.models.AggregateKey;
import io.smallrye.mutiny.Uni;

public interface AggregateCache {
  <T extends Aggregate> AggregateState<T> get(AggregateKey<T> aggregateKey);

  <T extends Aggregate> void put(AggregateKey<T> aggregateKey, AggregateState<T> aggregate);

  default void start(Class<? extends Aggregate> aggregateClass) {

  }

  default Uni<Void> setup(Class<? extends Aggregate> aggregateClass) {
    return Uni.createFrom().voidItem();
  }

  default Uni<Void> close() {
    return Uni.createFrom().voidItem();
  }
}
