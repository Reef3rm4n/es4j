package io.vertx.eventx.infrastructure.pg.mappers;

import io.vertx.eventx.infrastructure.pg.models.AggregateSnapshotRecord;
import io.vertx.eventx.sql.RecordMapper;
import io.vertx.eventx.sql.generator.filters.QueryBuilder;
import io.vertx.eventx.sql.models.EmptyQuery;
import io.vertx.eventx.infrastructure.pg.models.AggregateRecordKey;
import io.vertx.sqlclient.Row;

import java.util.Map;
import java.util.Set;

public class AggregateSnapshotMapper implements RecordMapper<AggregateRecordKey, AggregateSnapshotRecord, EmptyQuery> {

  public static final AggregateSnapshotMapper INSTANCE = new AggregateSnapshotMapper();
  private static final String ENTITY_ID = "entity_id";
  private static final String EVENT_VERSION = "event_version";
  private static final String STATE = "state";
  public static final String SNAPSHOTS = "snapshots";

  @Override
  public String table() {
    return SNAPSHOTS;
  }

  @Override
  public Set<String> columns() {
    return Set.of(ENTITY_ID, EVENT_VERSION, STATE);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(ENTITY_ID);
  }

  @Override
  public AggregateSnapshotRecord rowMapper(Row row) {
    return new AggregateSnapshotRecord(
      row.getString(ENTITY_ID),
      row.getLong(EVENT_VERSION),
      row.getJsonObject(STATE),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, AggregateSnapshotRecord actualRecord) {
    params.put(ENTITY_ID, actualRecord.entityId());
    params.put(EVENT_VERSION, actualRecord.eventVersion());
    params.put(STATE, actualRecord.state());
  }

  @Override
  public void keyParams(Map<String, Object> params, AggregateRecordKey key) {
    params.put(ENTITY_ID, key.aggregateId());
  }

  @Override
  public void queryBuilder(EmptyQuery query, QueryBuilder builder) {

  }

}

