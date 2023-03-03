package io.vertx.eventx.infrastructure.vertx;

import io.vertx.core.json.JsonObject;
import io.vertx.eventx.Aggregate;

import io.vertx.eventx.infrastructure.AggregateCache;
import io.vertx.eventx.infrastructure.models.AggregateKey;

import io.vertx.eventx.infrastructure.pg.models.AggregateRecordKey;
import io.vertx.eventx.objects.EntityState;
import io.vertx.mutiny.core.Vertx;

public class VertxCache<T extends Aggregate> implements AggregateCache<T> {

  private final VertxAggregateCache<T, EntityState<T>> cache;

  public VertxCache(
    Class<T> aggregateClass,
    Vertx vertx,
    Long ttl
  ) {
    this.cache = new VertxAggregateCache<>(
      vertx,
      aggregateClass,
      ttl
    );
  }

  @Override
  public EntityState<T> get(AggregateKey<T> aggregateKey) {
    return cache.get(new AggregateRecordKey(aggregateKey.aggregateId(), aggregateKey.tenantId()));
  }

  @Override
  public void put(EntityState<T> aggregate) {

  }
}
