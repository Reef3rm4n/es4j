package io.vertx.eventx.infrastructure.models;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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
  Class<? extends Event> startFrom,
  Integer maxSize
) {
  public JsonObject toJson() {
    return new JsonObject()
      .put("aggregate", aggregate)
      .put("aggregateId", aggregateId)
      .put("tenantId", tenantId)
      .put("eventVersionOffset", eventVersionOffset)
      .put("journalOffset", journalOffset)
      .put("startFrom", startFrom)
      .put("maxSize", maxSize);
  }
}
