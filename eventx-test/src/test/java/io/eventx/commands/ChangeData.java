package io.eventx.commands;

import io.eventx.Command;
import io.eventx.core.objects.CommandHeaders;

import java.util.Map;

public record ChangeData(
  String aggregateId,

  Map<String, Object> newData1,
  CommandHeaders headers

) implements Command {



}
