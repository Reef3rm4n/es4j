package io.vertx.eventx.infrastructure.models;

import io.vertx.eventx.Aggregate;

public record AggregateKey<T extends Aggregate>(
  Class<T> aggregateClass,
  String aggregateId,
  String tenantId
) {
}
