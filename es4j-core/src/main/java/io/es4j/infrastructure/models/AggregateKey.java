package io.es4j.infrastructure.models;

import io.es4j.Aggregate;
import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record AggregateKey<T extends Aggregate>(
  Class<T> aggregateClass,
  String aggregateId,
  String tenantId
) {
}
