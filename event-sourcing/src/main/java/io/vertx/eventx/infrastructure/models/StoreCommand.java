package io.vertx.eventx.infrastructure.models;

import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Command;
import io.vertx.eventx.common.EventxError;

public record StoreCommand<C extends Command, T extends Aggregate>(
  Class<T> aggregateClass,
  String entityId,
  C command,
  EventxError error
) {
}
