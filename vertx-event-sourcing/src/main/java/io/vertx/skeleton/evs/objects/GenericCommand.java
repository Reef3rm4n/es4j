package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.models.CommandHeaders;

import java.util.Map;

public record GenericCommand(
  String entityId,
  String commandClass,
  Map<String, Object> command,
  CommandHeaders commandHeaders
) implements Command {
}
