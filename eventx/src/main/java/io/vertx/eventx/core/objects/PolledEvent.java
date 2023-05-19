package io.vertx.eventx.core.objects;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.Event;

@RecordBuilder
public record PolledEvent(
  String aggregateId,
  String tenantId,
  Long journalOffset,
  Long aggregateOffset,
  Event event
) {


  public JsonObject toJson() {
    return new JsonObject()
      .put("aggregateId", aggregateId)
      .put("tenantId", tenantId)
      .put("journalOffset", journalOffset)
      .put("aggregateOffset", aggregateOffset)
      .put("event", JsonObject.mapFrom(event));
  }
}
