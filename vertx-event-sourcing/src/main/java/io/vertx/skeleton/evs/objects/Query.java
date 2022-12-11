package io.vertx.skeleton.evs.objects;

import io.vertx.core.json.JsonObject;

public record Query(
  String className,
  JsonObject query
) {
}
