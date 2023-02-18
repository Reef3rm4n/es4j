package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.models.RequestMetadata;

import java.util.Map;

public record GenericCommand(
  String entityId,
  String commandClass,
  Map<String,Object> command,
  RequestMetadata requestMetadata
) implements Command {
}
