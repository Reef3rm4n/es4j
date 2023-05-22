package io.eventx.infrastructure;

import io.eventx.Aggregate;
import io.eventx.core.objects.AggregateState;
import io.eventx.infrastructure.models.AggregateKey;
import io.smallrye.mutiny.Uni;

public interface SnapshotStore {
  <T extends Aggregate> Uni<AggregateState<T>> get(AggregateKey<T> key);

  <T extends Aggregate> Uni<Void> add(AggregateState<T> value);

  <T extends Aggregate> Uni<Void> update(AggregateState<T> aggregate);

  Uni<Void> close();
  Uni<Void> start();

}
