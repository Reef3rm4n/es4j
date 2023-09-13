package io.es4j.saga.commands;

import io.es4j.Command;

import java.util.Map;

public record ChangeDataWithDbConfig(
  String aggregateId,

  Map<String, Object> newData

) implements Command {



}
