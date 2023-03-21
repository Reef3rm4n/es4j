package io.vertx.eventx.objects;

import io.vertx.core.json.JsonObject;

public record Event(
  String entityId,
  String eventType,
  JsonObject event
) {
}
