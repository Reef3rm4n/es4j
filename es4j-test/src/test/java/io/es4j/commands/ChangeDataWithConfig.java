package io.es4j.commands;

import io.es4j.Command;

import java.util.Map;

public record ChangeDataWithConfig(
  String aggregateId,

  Map<String, Object> newData

) implements Command {



}
