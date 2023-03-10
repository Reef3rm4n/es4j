package io.vertx.eventx.infrastructure;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.infrastructure.models.AggregateKey;
import io.vertx.eventx.objects.AggregateState;

public interface AggregateCache {
  <T extends Aggregate> AggregateState<T> get(AggregateKey<T> aggregateKey);

  <T extends Aggregate> void put(AggregateKey<T> aggregateKey, AggregateState<T> aggregate);
  default Uni<Void> start() {
    return Uni.createFrom().voidItem();
  }
  default Uni<Void> close() {
    return Uni.createFrom().voidItem();
  }
}
