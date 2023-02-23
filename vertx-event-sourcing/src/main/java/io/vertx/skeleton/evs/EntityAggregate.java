package io.vertx.skeleton.evs;

import io.vertx.core.shareddata.Shareable;

public interface EntityAggregate extends Shareable {

  String entityId();

  default String tenantID() {
    return "default";
  }

}
