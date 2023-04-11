package io.vertx.eventx;


import io.vertx.eventx.objects.CommandHeaders;
import io.vertx.eventx.objects.CommandOptions;

public interface Command {
  String aggregateId();

  CommandHeaders headers();

  default CommandOptions options() {
    return CommandOptions.defaultOptions();
  }

  // todo add command options that enable command scheduling
  // todo add command options that enable repeating a command with crontab-like capabilities.


}
