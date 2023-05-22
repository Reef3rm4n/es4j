package io.eventx.core.objects;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;


@RecordBuilder
public record Event(
  String entityId,
  String eventType,
  JsonObject event
) {
}
