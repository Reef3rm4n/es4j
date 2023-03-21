package io.vertx.eventx.test.eventsourcing.domain.commands;

import io.vertx.eventx.Command;
import io.vertx.eventx.objects.CommandHeaders;

import java.util.Map;

public record CreateData(
  String aggregateId,
  Map<String, Object> data,
  CommandHeaders headers
) implements Command {
}
