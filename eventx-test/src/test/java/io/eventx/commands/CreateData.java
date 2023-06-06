package io.eventx.commands;

import io.eventx.Command;
import io.eventx.core.objects.CommandHeaders;

import java.util.Map;

public record CreateData(
  String aggregateId,
  Map<String, Object> data,
  CommandHeaders headers
) implements Command {
}
