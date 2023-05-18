package io.vertx.eventx.infrastructure.models;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Objects;

@RecordBuilder
public record Event(
  Long journalOffset,
  String aggregateId,
  String eventClass,
  Long eventVersion,
  JsonObject event,
  String tenantId,
  String commandId,
  List<String> tags,
  Integer schemaVersion
) {


  public Event(String aggregateId, String eventClass, Long eventVersion, JsonObject event, String tenantId, String commandId, List<String> tags, Integer schemaVersion) {
    this(null, aggregateId, eventClass, eventVersion, event, tenantId, commandId, tags, schemaVersion);
  }

  public Event {
    Objects.requireNonNull(aggregateId, "aggregateId must not be null");
    Objects.requireNonNull(eventClass, "Event class must not be null");
    Objects.requireNonNull(eventVersion, "Event versionTo must not be null");
    if (eventVersion < 0) {
      throw new IllegalArgumentException("Event versionTo must be greater than 0");
    }
    Objects.requireNonNull(event);
  }

}
