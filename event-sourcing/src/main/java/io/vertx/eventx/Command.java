package io.vertx.eventx;


import io.vertx.eventx.common.CommandHeaders;

public interface Command {
  String entityId();
  CommandHeaders headers();

  // todo add command options that enable command scheduling
  // todo add command options that enable repeating a command with crontab-like capabilities.


}
