package io.eventx;


import io.eventx.core.objects.CommandHeaders;
import io.eventx.core.objects.CommandOptions;

public interface Command {
  String aggregateId();

  CommandHeaders headers();

  default CommandOptions options() {
    return CommandOptions.defaultOptions();
  }

  // todo add command options that enable command scheduling
  // todo add command options that enable repeating a command with crontab-like capabilities.


}
