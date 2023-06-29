package io.eventx.infrastructure.models;

import io.eventx.Aggregate;
import io.eventx.Event;
import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;

@RecordBuilder
public record AggregateEventStream<T extends Aggregate>(
  String aggregateId,
  String tenantId,
  Long eventVersionOffset,
  Long journalOffset,
  Class<? extends Event> startFrom,
  Integer maxSize
) {
  public JsonObject toJson() {
    return new JsonObject()
      .put("aggregateId", aggregateId)
      .put("tenant", tenantId)
      .put("eventVersionOffset", eventVersionOffset)
      .put("journalOffset", journalOffset)
      .put("startFrom", startFrom)
      .put("maxSize", maxSize);
  }
}
