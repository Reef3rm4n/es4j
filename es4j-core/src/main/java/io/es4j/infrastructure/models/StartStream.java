package io.es4j.infrastructure.models;

import io.es4j.Aggregate;
import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record StartStream<T extends Aggregate>(
  Class<T> aggregate,
  String aggregateId,
  String tenantId
) {
}
