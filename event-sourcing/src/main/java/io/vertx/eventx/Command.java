package io.vertx.eventx;


import io.vertx.eventx.common.CommandHeaders;
import io.vertx.eventx.common.CommandOptions;

public interface Command {
  String entityId();

  CommandHeaders headers();

  default CommandOptions options() {
    return CommandOptions.defaultOptions();
  }

  // todo add command options that enable command scheduling
  // todo add command options that enable repeating a command with crontab-like capabilities.


}
