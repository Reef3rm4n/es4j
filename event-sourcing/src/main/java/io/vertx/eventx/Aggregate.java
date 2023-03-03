package io.vertx.eventx;

import io.vertx.core.shareddata.Shareable;

import java.util.Optional;

public interface Aggregate extends Shareable {

  String aggregateId();

  default String tenantID() {
    return "default";
  }

  default Optional<Class<? extends Command>> finalState() {
    return Optional.empty();
  }

}
