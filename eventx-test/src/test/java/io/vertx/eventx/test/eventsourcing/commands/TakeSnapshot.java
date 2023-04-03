package io.vertx.eventx.test.eventsourcing.commands;

import io.vertx.eventx.Command;
import io.vertx.eventx.objects.CommandHeaders;

public record TakeSnapshot(
  String aggregateId,
  CommandHeaders headers
) implements Command {
}
