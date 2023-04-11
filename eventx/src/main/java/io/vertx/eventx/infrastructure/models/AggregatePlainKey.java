package io.vertx.eventx.infrastructure.models;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record AggregatePlainKey(
  String aggregateClass,
  String aggregateId,
  String tenantId
) {
}
