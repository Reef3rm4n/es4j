package io.es4j.infrastructure.models;

import io.es4j.Aggregate;
import io.es4j.Event;
import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;

@RecordBuilder
public record AggregateEventStream<T extends Aggregate>(
  String aggregateId,
  String tenantId,
  Long eventVersionOffset,
  Long journalOffset,
  Boolean startFromSnapshot,
  Integer maxSize
) {
  public JsonObject toJson() {
    return new JsonObject()
      .put("aggregateId", aggregateId)
      .put("tenant", tenantId)
      .put("eventVersionOffset", eventVersionOffset)
      .put("journalOffset", journalOffset)
      .put("startFromSnapshot", startFromSnapshot)
      .put("maxSize", maxSize);
  }
}
