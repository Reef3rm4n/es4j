package io.vertx.eventx.infra.pg.models;

import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Shareable;
import io.vertx.eventx.sql.models.BaseRecord;
import io.vertx.eventx.sql.models.RepositoryRecord;

import java.util.List;
import java.util.Objects;

public record EventRecord(
  Long id,
  String aggregateClass,
  String aggregateId,
  String eventClass,
  Long eventVersion,
  JsonObject event,
  String commandId,
  List<String> tags,
  Integer schemaVersion,
  BaseRecord baseRecord
) implements RepositoryRecord<EventRecord>, Shareable {

  public EventRecord(String aggregateClass, String entityId, String eventClass, Long eventVersion, JsonObject event, String commandId, List<String> tags, Integer schemaVersion, BaseRecord baseRecord) {
    this(null, aggregateClass, entityId, eventClass, eventVersion, event, commandId, tags, schemaVersion, baseRecord);
  }

  public EventRecord {
    Objects.requireNonNull(aggregateClass, "Entity must not be null");
    Objects.requireNonNull(aggregateId, "Aggregate must not be null");
    Objects.requireNonNull(eventClass, "Event class must not be null");
    Objects.requireNonNull(eventVersion, "Event version must not be null");
    if (eventVersion < 0) {
      throw new IllegalArgumentException("Event version must be greater than 0");
    }
    Objects.requireNonNull(event);
  }


  @Override
  public EventRecord with(BaseRecord baseRecord) {
    return new EventRecord(id, aggregateClass, aggregateId, eventClass, eventVersion, event, commandId, tags, schemaVersion, baseRecord);
  }
}
