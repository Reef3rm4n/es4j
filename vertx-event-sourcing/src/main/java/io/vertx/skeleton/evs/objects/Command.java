package io.vertx.skeleton.evs.objects;

import io.vertx.core.json.JsonObject;

public record Command(
  String commandClass,
  JsonObject command
) {
}
