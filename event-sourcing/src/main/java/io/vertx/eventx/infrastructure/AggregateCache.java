package io.vertx.eventx.infrastructure;

import io.vertx.eventx.Aggregate;
import io.vertx.eventx.infrastructure.models.AggregateKey;
import io.vertx.eventx.objects.EntityState;

public interface AggregateCache<T extends Aggregate> {
  EntityState<T> get(AggregateKey<T> aggregateKey);

  void put(EntityState<T> aggregate);
}
