package io.vertx.eventx.objects;

import io.vertx.core.json.JsonObject;

public record Command(
  String commandType,
  JsonObject command
) {
}
