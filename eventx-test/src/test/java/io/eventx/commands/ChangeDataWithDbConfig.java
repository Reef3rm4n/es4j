package io.eventx.commands;

import io.eventx.Command;
import io.eventx.core.objects.CommandHeaders;

import java.util.Map;

public record ChangeDataWithDbConfig(
  String aggregateId,

  Map<String, Object> newData,
  CommandHeaders headers

) implements Command {



}
