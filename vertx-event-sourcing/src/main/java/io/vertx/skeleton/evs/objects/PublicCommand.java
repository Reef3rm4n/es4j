package io.vertx.skeleton.evs.objects;

import java.util.Map;

public record PublicCommand(
  String entityId,
  String commandClass,
  Map<String, Object> command
) {
}
