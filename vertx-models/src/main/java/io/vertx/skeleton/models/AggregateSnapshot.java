package io.vertx.skeleton.models;

import io.vertx.core.json.JsonObject;

public record AggregateSnapshot(
  String entityId,
  Long eventVersion,
  JsonObject state,
  PersistedRecord persistedRecord
) implements RepositoryRecord<AggregateSnapshot> {


  @Override
  public AggregateSnapshot with(final PersistedRecord persistedRecord) {
    return new AggregateSnapshot(entityId, eventVersion, state, persistedRecord);
  }

  public AggregateSnapshot withState(final JsonObject mapFrom) {
    return new AggregateSnapshot(entityId, eventVersion, mapFrom, persistedRecord);
  }
}
