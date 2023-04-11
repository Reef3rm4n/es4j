package io.vertx.eventx.infrastructure.models;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Event;

import java.util.List;

@RecordBuilder
public record AggregateEventStream<T extends Aggregate>(
  Class<T> aggregate,
  String aggregateId,
  String tenantId,
  Long eventVersionOffset,
  Long journalOffset,
  Class<? extends Event> startFrom
) {
}
