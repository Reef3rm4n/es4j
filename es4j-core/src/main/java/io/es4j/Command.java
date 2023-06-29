package io.es4j;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.es4j.core.objects.CommandOptions;
import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public interface Command extends Shareable, Serializable {
  String aggregateId();

  default String tenant() {
    return "default";
  }

  default String uniqueId() {
    return UUID.randomUUID().toString();
  }

  default List<String> requiredRoles() {
    return Collections.emptyList();
  }

  default CommandOptions options() {
    return CommandOptions.defaultOptions();
  }


}
