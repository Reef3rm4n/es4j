package io.eventx.config;

import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;

public interface Configuration extends Shareable, Serializable {

  default String tenant() {
    return "default";
  }

  String name();

  default Integer revision() {
    return 0;
  }

  default String description() {
    return "";
  }

  default Boolean active() {
    return true;
  }
}
