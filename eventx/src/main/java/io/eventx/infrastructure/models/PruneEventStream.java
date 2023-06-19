package io.eventx.infrastructure.models;

import io.eventx.Aggregate;
import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record PruneEventStream<T extends Aggregate>(
  Class<T> aggregate,
  String aggregateId,
  String tenantId,
  Long offsetTo
) {

}
