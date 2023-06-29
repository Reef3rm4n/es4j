package io.es4j.commands;

import io.es4j.Command;
import io.es4j.core.objects.CommandHeaders;

import java.util.Map;

public record ChangeDataWithConfig(
  String aggregateId,

  Map<String, Object> newData,
  CommandHeaders headers

) implements Command {



}
