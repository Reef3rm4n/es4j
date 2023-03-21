package io.vertx.eventx.objects;

import io.vertx.eventx.Event;

public record PolledEvent(
  String aggregateClass,
  String aggregateId,
  String tenantId,
  Long journalOffset,
  Long aggregateOffset,
  Event event
) {
}
