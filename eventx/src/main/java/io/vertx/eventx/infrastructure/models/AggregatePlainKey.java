package io.vertx.eventx.infrastructure.models;

public record AggregatePlainKey(
  String aggregateClass,
  String aggregateId,
  String tenantId
) {
}
