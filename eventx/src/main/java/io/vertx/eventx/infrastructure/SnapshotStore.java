package io.vertx.eventx.infrastructure;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.objects.AggregateState;
import io.vertx.eventx.infrastructure.models.AggregateKey;

public interface SnapshotStore {
  <T extends Aggregate> Uni<AggregateState<T>> get(AggregateKey<T> key);

  <T extends Aggregate> Uni<Void> add(AggregateState<T> value);

  <T extends Aggregate> Uni<Void> update(AggregateState<T> aggregate);

  Uni<Void> close();
  Uni<Void> start();

}
