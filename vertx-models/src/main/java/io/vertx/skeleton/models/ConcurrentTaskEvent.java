package io.vertx.skeleton.models;

import io.vertx.core.json.JsonObject;

public record ConcurrentTaskEvent(
  String id,
  TaskEventType type,
  JsonObject payload,
  JsonObject error,
  PersistedRecord persistedRecord
) implements RepositoryRecord<ConcurrentTaskEvent> {

  @Override
  public ConcurrentTaskEvent with(final PersistedRecord persistedRecord) {
    return new ConcurrentTaskEvent(id, type, payload, error, persistedRecord);
  }

}
