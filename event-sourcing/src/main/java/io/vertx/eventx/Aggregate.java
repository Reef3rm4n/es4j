package io.vertx.eventx;

import io.vertx.core.shareddata.Shareable;

public interface Aggregate extends Shareable {

  String entityId();

  default String tenantID() {
    return "default";
  }

}
