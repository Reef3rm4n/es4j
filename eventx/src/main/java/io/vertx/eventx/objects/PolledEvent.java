package io.vertx.eventx.objects;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.eventx.Event;

@RecordBuilder
public record PolledEvent(
  String aggregateClass,
  String aggregateId,
  String tenantId,
  Long journalOffset,
  Long aggregateOffset,
  Event event
) {
}
