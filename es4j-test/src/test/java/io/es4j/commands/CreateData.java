package io.es4j.commands;

import io.es4j.Command;

import java.util.Map;

public record CreateData(
  String aggregateId,
  Map<String, Object> data
) implements Command {
}
