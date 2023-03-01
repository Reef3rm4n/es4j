package io.vertx.eventx.config;

import io.vertx.core.shareddata.Shareable;

public interface ConfigurationEntry extends Shareable {

  default String tenant() {
    return "default";
  }
  String name();
}
