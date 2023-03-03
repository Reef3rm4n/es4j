package io.vertx.eventx.infrastructure.models;

public record AggregateKey<T>(
  Class<T> aggregateClass,
  String aggregateId,
  String tenantId
) {
}
