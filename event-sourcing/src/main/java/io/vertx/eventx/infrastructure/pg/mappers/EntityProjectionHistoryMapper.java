package io.vertx.eventx.infrastructure.pg.mappers;

import io.vertx.eventx.infrastructure.pg.models.ProjectionHistoryQuery;
import io.vertx.eventx.infrastructure.pg.models.ProjectionOffset;
import io.vertx.eventx.sql.RecordMapper;
import io.vertx.eventx.sql.generator.filters.QueryBuilder;
import io.vertx.eventx.sql.models.QueryFilters;
import io.vertx.eventx.infrastructure.pg.models.ProjectionHistoryKey;
import io.vertx.sqlclient.Row;

import java.util.Map;
import java.util.Set;

public class EntityProjectionHistoryMapper implements RecordMapper<ProjectionHistoryKey, ProjectionOffset, ProjectionHistoryQuery> {

  public static final String PROJECTION_HISTORY = "projection_history";
  public static final String PROJECTION_CLASS = "projection_class";
  public static final String ENTITY_ID = "entity_id";
  public static final String LAST_EVENT_VERSION = "last_event_version";

  private EntityProjectionHistoryMapper(){}

  public static final EntityProjectionHistoryMapper INSTANCE = new EntityProjectionHistoryMapper();
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
  public ProjectionOffset rowMapper(Row row) {
    return new ProjectionOffset(
      row.getString(ENTITY_ID),
      row.getString(PROJECTION_CLASS),
      row.getLong(LAST_EVENT_VERSION),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, ProjectionOffset actualRecord) {
    params.put(PROJECTION_CLASS, actualRecord.projectionClass());
    params.put(ENTITY_ID, actualRecord.entityId());
    params.put(LAST_EVENT_VERSION, actualRecord.lastAggregateVersion());
  }

  @Override
  public void keyParams(Map<String, Object> params, ProjectionHistoryKey key) {
    params.put(ENTITY_ID, key.entityId());
    params.put(PROJECTION_CLASS, key.projectionClass());
  }

  @Override
  public void queryBuilder(ProjectionHistoryQuery query, QueryBuilder builder) {
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
