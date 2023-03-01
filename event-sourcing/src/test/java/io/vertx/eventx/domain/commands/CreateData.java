package io.vertx.eventx.domain.commands;

import io.vertx.eventx.Command;
import io.vertx.eventx.common.CommandHeaders;

import java.util.Map;

public record CreateData(
  String entityId,
  Map<String, Object> data,
  CommandHeaders headers
) implements Command {
}
