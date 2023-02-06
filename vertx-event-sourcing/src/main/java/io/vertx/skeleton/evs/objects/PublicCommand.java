package io.vertx.skeleton.evs.objects;

import java.util.Map;

public record PublicCommand(
  String commandType,
  Map<String, Object> command
) {
}
