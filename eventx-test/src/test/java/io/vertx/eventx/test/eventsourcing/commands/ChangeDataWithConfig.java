package io.vertx.eventx.test.eventsourcing.commands;

import io.vertx.eventx.Command;
import io.vertx.eventx.core.objects.CommandHeaders;

import java.util.Map;

public record ChangeDataWithConfig(
  String aggregateId,

  Map<String, Object> newData,
  CommandHeaders headers

) implements Command {



}
