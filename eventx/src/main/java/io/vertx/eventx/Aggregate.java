package io.vertx.eventx;

import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;
import java.util.Optional;

public interface Aggregate extends Shareable, Serializable {

  String aggregateId();

  default String tenantID() {
    return "default";
  }

  default Optional<Class<? extends Event>> snapshotOn() {
    return Optional.empty();
  }

}
