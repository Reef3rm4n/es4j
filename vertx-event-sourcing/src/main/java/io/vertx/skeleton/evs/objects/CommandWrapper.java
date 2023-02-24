package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.models.CommandHeaders;

public record CommandWrapper(
  String entityId,
  io.vertx.skeleton.evs.objects.Command command,
  CommandHeaders commandHeaders
) implements Command {

}
