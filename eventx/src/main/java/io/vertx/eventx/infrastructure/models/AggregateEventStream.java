package io.vertx.eventx.infrastructure.models;

import io.vertx.eventx.Aggregate;


public record AggregateEventStream<T extends Aggregate>(
  Class<T> aggregate,
  String aggregateId,
  String tenantId,
  Long eventVersionOffset,
  Long journalOffset
) {
}
