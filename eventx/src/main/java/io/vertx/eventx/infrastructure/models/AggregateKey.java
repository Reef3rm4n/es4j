package io.vertx.eventx.infrastructure.models;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.eventx.Aggregate;
@RecordBuilder
public record AggregateKey<T extends Aggregate>(
  Class<T> aggregateClass,
  String aggregateId,
  String tenantId
) {
}
