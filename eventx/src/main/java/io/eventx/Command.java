package io.eventx;


import io.eventx.core.objects.CommandHeaders;
import io.eventx.core.objects.CommandOptions;
import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public interface Command extends Shareable, Serializable {
  String aggregateId();

  default CommandHeaders headers() {
    return CommandHeaders.defaultHeaders();
  }

  default CommandOptions options() {
    return CommandOptions.defaultOptions();
  }

  default List<String> requiredRoles() {
    return Collections.emptyList();
  }

}
