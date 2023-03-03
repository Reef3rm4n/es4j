package io.vertx.eventx.infrastructure.models;

import io.vertx.core.json.JsonObject;

import java.util.Objects;

public record Event(
  String entityId,
  String eventClass,
  Long eventVersion,
  JsonObject event,
  String tenantId
) {

  public Event {
    Objects.requireNonNull(entityId, "Entity must not be null");
    Objects.requireNonNull(eventClass, "Event class must not be null");
    Objects.requireNonNull(eventVersion, "Event version must not be null");
    if (eventVersion < 0) {
      throw new IllegalArgumentException("Event version must be greater than 0");
    }
    Objects.requireNonNull(event);
  }

}
