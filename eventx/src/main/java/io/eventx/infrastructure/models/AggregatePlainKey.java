package io.eventx.infrastructure.models;

import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.Objects;

@RecordBuilder
public record AggregatePlainKey(
  String aggregateClass,
  String aggregateId,
  String tenantId
) {

  public AggregatePlainKey(
    String aggregateClass,
    String aggregateId,
    String tenantId
  ) {
    this.aggregateClass = Objects.requireNonNull(aggregateClass, "Aggregate must have a class");
    this.aggregateId = Objects.requireNonNull(aggregateId, "Aggregate must have an ID");
    this.tenantId = Objects.requireNonNullElse(tenantId, "default");
  }

}
