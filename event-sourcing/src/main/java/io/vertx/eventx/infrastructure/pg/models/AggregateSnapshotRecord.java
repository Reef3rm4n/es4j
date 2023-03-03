package io.vertx.eventx.infrastructure.pg.models;

import io.vertx.core.json.JsonObject;
import io.vertx.eventx.sql.models.RepositoryRecord;
import io.vertx.eventx.sql.models.BaseRecord;

public record AggregateSnapshotRecord(
  String entityId,
  Long eventVersion,
  JsonObject state,
  BaseRecord baseRecord
) implements RepositoryRecord<AggregateSnapshotRecord> {


  @Override
  public AggregateSnapshotRecord with(final BaseRecord baseRecord) {
    return new AggregateSnapshotRecord(entityId, eventVersion, state, baseRecord);
  }

  public AggregateSnapshotRecord withState(final JsonObject newState) {
    return new AggregateSnapshotRecord(entityId, eventVersion, newState, baseRecord);
  }
}
