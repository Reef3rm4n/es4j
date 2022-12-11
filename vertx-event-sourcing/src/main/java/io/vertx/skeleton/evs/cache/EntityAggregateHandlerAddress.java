package io.vertx.skeleton.evs.cache;

import io.vertx.skeleton.models.EntityAggregateKey;

public record EntityAggregateHandlerAddress(
  EntityAggregateKey aggregateKey,
  String address
) {
}
