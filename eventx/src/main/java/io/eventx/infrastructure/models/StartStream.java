package io.eventx.infrastructure.models;

import io.eventx.Aggregate;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.List;

@RecordBuilder
public record StartStream<T extends Aggregate>(
  Class<T> aggregate,
  String aggregateId,
  String tenantId
) {
}
