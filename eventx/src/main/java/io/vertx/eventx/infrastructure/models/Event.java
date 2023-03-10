package io.vertx.eventx.infrastructure.models;

import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Objects;

public record Event(
  Long journalOffset,
  String aggregateClass,
  String aggregateId,
  String eventClass,
  Long eventVersion,
  JsonObject event,
  String tenantId,
  String commandId,
  List<String> tags,
  Integer schemaVersion
) {


  public Event(String aggregateClass, String aggregateId, String eventClass, Long eventVersion, JsonObject event, String tenantId, String commandId, List<String> tags, Integer schemaVersion) {
    this(null, aggregateClass, aggregateId, eventClass, eventVersion, event, tenantId, commandId, tags, schemaVersion);
  }

  public Event {
    Objects.requireNonNull(aggregateClass, "Aggregate class must not be null");
    Objects.requireNonNull(aggregateId, "Entity must not be null");
    Objects.requireNonNull(eventClass, "Event class must not be null");
    Objects.requireNonNull(eventVersion, "Event version must not be null");
    if (eventVersion < 0) {
      throw new IllegalArgumentException("Event version must be greater than 0");
    }
    Objects.requireNonNull(event);
  }

}
