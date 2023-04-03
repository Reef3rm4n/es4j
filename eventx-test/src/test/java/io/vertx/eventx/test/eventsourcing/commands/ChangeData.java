package io.vertx.eventx.test.eventsourcing.commands;

import io.vertx.eventx.Command;
import io.vertx.eventx.objects.CommandHeaders;

import java.util.Map;

public record ChangeData(
  String aggregateId,

  Map<String, Object> newData1,
  CommandHeaders headers

) implements Command {



}
