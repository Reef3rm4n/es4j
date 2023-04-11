package io.vertx.eventx.objects;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;

@RecordBuilder
public record Command(
  String commandType,
  JsonObject command
) {
}
