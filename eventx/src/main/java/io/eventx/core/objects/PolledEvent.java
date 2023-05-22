package io.eventx.core.objects;

import io.eventx.Event;
import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;

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
