package io.vertx.skeleton.evs.mappers;

import io.vertx.skeleton.evs.objects.EntityProjectionHistory;
import io.vertx.skeleton.evs.objects.EntityProjectionHistoryKey;
import io.vertx.skeleton.evs.objects.EntityProjectionHistoryQuery;
import io.vertx.skeleton.sql.RecordMapper;
import io.vertx.skeleton.sql.generator.filters.QueryBuilder;
import io.vertx.skeleton.sql.models.QueryFilters;
import io.vertx.sqlclient.Row;

import java.util.Map;
import java.util.Set;

public class EntityProjectionHistoryMapper implements RecordMapper<EntityProjectionHistoryKey, EntityProjectionHistory, EntityProjectionHistoryQuery> {

  public static final String PROJECTION_HISTORY = "projection_history";
  public static final String PROJECTION_CLASS = "projection_class";
  public static final String ENTITY_ID = "entity_id";
  public static final String LAST_EVENT_VERSION = "last_event_version";

  @Override
  public String table() {
    return PROJECTION_HISTORY;
  }

  @Override
  public Set<String> columns() {
    return Set.of(PROJECTION_CLASS, ENTITY_ID, LAST_EVENT_VERSION);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(PROJECTION_CLASS, ENTITY_ID);
  }

  @Override
  public EntityProjectionHistory rowMapper(Row row) {
    return new EntityProjectionHistory(
      row.getString(ENTITY_ID),
      row.getString(PROJECTION_CLASS),
      row.getLong(LAST_EVENT_VERSION),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, EntityProjectionHistory actualRecord) {
    params.put(PROJECTION_CLASS, actualRecord.projectionClass());
    params.put(ENTITY_ID, actualRecord.entityId());
    params.put(LAST_EVENT_VERSION, actualRecord.lastEventVersion());
  }

  @Override
  public void keyParams(Map<String, Object> params, EntityProjectionHistoryKey key) {
    params.put(ENTITY_ID, key.entityId());
    params.put(PROJECTION_CLASS, key.projectionClass());
  }

  @Override
  public void queryBuilder(EntityProjectionHistoryQuery query, QueryBuilder builder) {
    builder
      .iLike(
        new QueryFilters<>(String.class)
          .filterColumn(PROJECTION_CLASS)
          .filterParams(query.projectionClasses())
      )
      .iLike(
        new QueryFilters<>(String.class)
          .filterColumn(ENTITY_ID)
          .filterParams(query.entityIds())
      );
  }
}
