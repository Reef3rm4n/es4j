package io.vertx.skeleton.evs.domain.commands;

import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.models.CommandHeaders;

public record Create(
  String entityId,
  CommandHeaders commandHeaders
) implements Command {
}
