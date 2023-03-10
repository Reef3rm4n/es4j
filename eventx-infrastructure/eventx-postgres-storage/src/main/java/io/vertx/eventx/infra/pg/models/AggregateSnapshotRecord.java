package io.vertx.eventx.infra.pg.models;

import io.vertx.core.json.JsonObject;
import io.vertx.eventx.sql.models.BaseRecord;
import io.vertx.eventx.sql.models.RepositoryRecord;

import java.util.List;

public record AggregateSnapshotRecord(
  String entityId,
  String aggregateClass,
  Long currentEventVersion,
  List<String> commandIds,
  JsonObject state,
  Long journalOffset,
  BaseRecord baseRecord
) implements RepositoryRecord<AggregateSnapshotRecord> {


  @Override
  public AggregateSnapshotRecord with(final BaseRecord baseRecord) {
    return new AggregateSnapshotRecord(entityId, aggregateClass, currentEventVersion, commandIds, state, journalOffset, baseRecord);
  }

  public AggregateSnapshotRecord withState(final Long currentEventVersion, final List<String> commandIds, final JsonObject newState) {
    return new AggregateSnapshotRecord(entityId, aggregateClass, currentEventVersion, commandIds, newState, journalOffset, baseRecord);
  }

}
