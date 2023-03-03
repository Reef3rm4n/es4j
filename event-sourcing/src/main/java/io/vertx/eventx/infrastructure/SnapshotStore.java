package io.vertx.eventx.infrastructure;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.objects.EntityState;
import io.vertx.eventx.infrastructure.models.AggregateKey;

public interface SnapshotStore {
  <T extends Aggregate> Uni<EntityState<T>> get(AggregateKey<T> key);

  <T extends Aggregate> Uni<Void> add(EntityState<T> value);

  <T extends Aggregate> Uni<Void> update(EntityState<T> aggregate);

}
