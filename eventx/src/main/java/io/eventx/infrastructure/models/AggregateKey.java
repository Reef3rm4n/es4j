package io.eventx.infrastructure.models;

import io.eventx.Aggregate;
import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record AggregateKey<T extends Aggregate>(
  Class<T> aggregateClass,
  String aggregateId,
  String tenantId
) {
}
