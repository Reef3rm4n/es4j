package io.es4j.infra.pg.models;

import io.es4j.sql.models.RepositoryRecord;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Shareable;
import io.es4j.sql.models.BaseRecord;

import java.util.List;
import java.util.Objects;

public record EventRecord(
  Long id,
  String aggregateId,
  String eventClass,
  Long eventVersion,
  JsonObject event,
  String commandId,
  List<String> tags,
  Integer schemaVersion,
  BaseRecord baseRecord
) implements RepositoryRecord<EventRecord>, Shareable {

  public EventRecord(String aggregateId, String eventClass, Long eventVersion, JsonObject event, String commandId, List<String> tags, Integer schemaVersion, BaseRecord baseRecord) {
    this(null, aggregateId, eventClass, eventVersion, event, commandId, tags, schemaVersion, baseRecord);
  }

  public EventRecord {
    Objects.requireNonNull(aggregateId, "aggregateId must not be null");
    Objects.requireNonNull(eventClass, "eventType must not be null");
    Objects.requireNonNull(eventVersion, "eventVersion must not be null");
    if (eventVersion < 0) {
      throw new IllegalArgumentException("eventVersion must be greater than 0");
    }
    Objects.requireNonNull(event, "event must not be null");
  }


  @Override
  public EventRecord with(BaseRecord baseRecord) {
    return new EventRecord(id, aggregateId, eventClass, eventVersion, event, commandId, tags, schemaVersion, baseRecord);
  }
}
