package io.eventx;


import io.eventx.core.objects.CommandHeaders;
import io.eventx.core.objects.CommandOptions;
import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;

public interface Command extends Shareable, Serializable {
  String aggregateId();

  default CommandHeaders headers() {
    return CommandHeaders.defaultHeaders();
  }

  default CommandOptions options() {
    return CommandOptions.defaultOptions();
  }

  // todo add command options that enable command scheduling
  // todo add command options that enable repeating a command with crontab-like capabilities.


}
