package io.vertx.skeleton.evs.objects;

import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.models.PersistedRecord;
import io.vertx.skeleton.models.RepositoryRecord;

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
