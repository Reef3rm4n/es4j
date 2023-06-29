package io.es4j.infrastructure;

import io.es4j.Aggregate;
import io.es4j.core.objects.AggregateConfiguration;
import io.es4j.core.objects.AggregateState;
import io.es4j.infrastructure.models.AggregateKey;
import io.smallrye.mutiny.Uni;

public interface AggregateCache {
  <T extends Aggregate> AggregateState<T> get(AggregateKey<T> aggregateKey);

  <T extends Aggregate> void put(AggregateKey<T> aggregateKey, AggregateState<T> aggregate);

  default Uni<Void> setup(Class<? extends Aggregate> aggregateClass, AggregateConfiguration aggregateConfiguration) {
    return Uni.createFrom().voidItem();
  }

  default Uni<Void> close() {
    return Uni.createFrom().voidItem();
  }
}
