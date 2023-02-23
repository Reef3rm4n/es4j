package io.vertx.skeleton.evs.objects;

import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.sql.models.RepositoryRecord;
import io.vertx.skeleton.sql.models.BaseRecord;

public record AggregateSnapshot(
  String entityId,
  Long eventVersion,
  JsonObject state,
  BaseRecord baseRecord
) implements RepositoryRecord<AggregateSnapshot> {


  @Override
  public AggregateSnapshot with(final BaseRecord baseRecord) {
    return new AggregateSnapshot(entityId, eventVersion, state, baseRecord);
  }

  public AggregateSnapshot withState(final JsonObject newState) {
    return new AggregateSnapshot(entityId, eventVersion, newState, baseRecord);
  }
}
