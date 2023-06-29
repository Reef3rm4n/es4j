package io.es4j.core.objects;

import io.es4j.Event;
import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;

@RecordBuilder
public record AggregateEvent(
  String aggregateId,
  String tenantId,
  Long journalOffset,
  Long aggregateOffset,
  Event event
) implements Serializable, Shareable {

  public JsonObject toJson() {
    return new JsonObject()
      .put("aggregateId", aggregateId)
      .put("tenant", tenantId)
      .put("journalOffset", journalOffset)
      .put("aggregateOffset", aggregateOffset)
      .put("event", JsonObject.mapFrom(event));
  }
}
