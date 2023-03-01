package io.vertx.eventx.domain.commands;

import io.vertx.eventx.Command;
import io.vertx.eventx.common.CommandHeaders;

import java.util.Map;

public record ChangeData(
  String entityId,

  Map<String, Object> newData1,
  CommandHeaders headers

) implements Command {



}
