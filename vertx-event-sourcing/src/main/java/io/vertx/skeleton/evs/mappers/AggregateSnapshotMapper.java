package io.vertx.skeleton.evs.mappers;

import io.vertx.skeleton.sql.RecordMapper;
import io.vertx.skeleton.sql.generator.filters.QueryBuilder;
import io.vertx.skeleton.evs.objects.AggregateSnapshot;
import io.vertx.skeleton.evs.objects.EntityKey;
import io.vertx.skeleton.orm.Constants;
import io.vertx.skeleton.sql.models.EmptyQuery;
import io.vertx.sqlclient.Row;

import java.util.Map;
import java.util.Set;

public class AggregateSnapshotMapper implements RecordMapper<EntityKey, AggregateSnapshot, EmptyQuery> {

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
    return Set.of(Constants.ENTITY_ID);
  }

  @Override
  public AggregateSnapshot rowMapper(Row row) {
    return new AggregateSnapshot(
      row.getString(Constants.ENTITY_ID),
      row.getLong(Constants.EVENT_VERSION),
      row.getJsonObject(Constants.STATE),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, AggregateSnapshot actualRecord) {
    params.put(Constants.ENTITY_ID, actualRecord.entityId());
    params.put(Constants.EVENT_VERSION, actualRecord.eventVersion());
    params.put(Constants.STATE, actualRecord.state());
  }

  @Override
  public void keyParams(Map<String, Object> params, EntityKey key) {
    params.put(ENTITY_ID, key.entityId());
  }

  @Override
  public void queryBuilder(EmptyQuery query, QueryBuilder builder) {

  }

}

