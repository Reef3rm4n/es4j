package io.eventx;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.eventx.core.objects.CommandHeaders;
import io.eventx.core.objects.CommandOptions;
import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public interface Command extends Shareable, Serializable {
  String aggregateId();

  default String tenant() {
    return "default";
  }

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
