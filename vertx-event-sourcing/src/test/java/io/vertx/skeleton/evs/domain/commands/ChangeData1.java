package io.vertx.skeleton.evs.domain.commands;

import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.models.CommandHeaders;

public record ChangeData1(
  String entityId,

  String newData1,
  CommandHeaders commandHeaders

) implements Command {



}
