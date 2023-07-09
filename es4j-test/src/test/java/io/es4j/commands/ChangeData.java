package io.es4j.commands;

import io.es4j.Command;

import java.util.Map;

public record ChangeData(
  String aggregateId,

  Map<String, Object> newData1

) implements Command {



}
