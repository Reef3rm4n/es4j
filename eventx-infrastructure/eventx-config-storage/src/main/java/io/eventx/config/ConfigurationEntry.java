package io.eventx.config;

import io.vertx.core.shareddata.Shareable;

public interface ConfigurationEntry extends Shareable {

  default String tenant() {
    return "default";
  }
  String name();

  default Integer revision() {
    return 0;
  }

  String description();
  default Boolean active() {
    return true;
  }
}
