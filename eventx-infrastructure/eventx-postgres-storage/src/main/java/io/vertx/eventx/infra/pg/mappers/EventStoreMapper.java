package io.vertx.eventx.infra.pg.mappers;

import io.vertx.eventx.infra.pg.models.EventRecord;
import io.vertx.eventx.infra.pg.models.EventRecordKey;
import io.vertx.eventx.infra.pg.models.EventRecordQuery;
import io.vertx.eventx.sql.RecordMapper;
import io.vertx.eventx.sql.generator.filters.QueryBuilder;
import io.vertx.eventx.sql.models.QueryFilter;
import io.vertx.eventx.sql.models.QueryFilters;
import io.vertx.sqlclient.Row;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class EventStoreMapper implements RecordMapper<EventRecordKey, EventRecord, EventRecordQuery> {
  public static final String TABLE = "event_store";
  private static final String AGGREGATE_ID = "aggregate_id";
  public static final String ID = "id";
  private static final String EVENT = "event";
  public static final String EVENT_VERSION = "event_version";
  private static final String EVENT_CLASS = "event_class";
  public static final EventStoreMapper INSTANCE = new EventStoreMapper();
  public static final String COMMAND_ID = "command_id";
  public static final String AGGREGATE_CLASS = "aggregate_class";
  public static final String TAGS = "tags";
  public static final String SCHEMA_VERSION = "schema_version";

  private EventStoreMapper() {
  }

  @Override
  public String table() {
    return TABLE;
  }

  @Override
  public Set<String> columns() {
    return Set.of(AGGREGATE_CLASS, AGGREGATE_ID, EVENT, EVENT_VERSION, EVENT_CLASS, TAGS, SCHEMA_VERSION, COMMAND_ID);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(ID);
  }

  @Override
  public EventRecord rowMapper(Row row) {
    return new EventRecord(
      row.getLong(ID),
      row.getString(AGGREGATE_CLASS),
      row.getString(AGGREGATE_ID),
      row.getString(EVENT_CLASS),
      row.getLong(EVENT_VERSION),
      row.getJsonObject(EVENT),
      row.getString(COMMAND_ID),
      row.getArrayOfStrings(TAGS) != null ? Arrays.stream(row.getArrayOfStrings(TAGS)).toList() : null,
      row.getInteger(SCHEMA_VERSION),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, EventRecord actualRecord) {
    params.put(ID, actualRecord.id());
    params.put(AGGREGATE_ID, actualRecord.aggregateId());
    params.put(AGGREGATE_CLASS, actualRecord.aggregateClass());
    params.put(EVENT_CLASS, actualRecord.eventClass());
    params.put(EVENT_VERSION, actualRecord.eventVersion());
    params.put(EVENT, actualRecord.event());
    params.put(COMMAND_ID, actualRecord.commandId());
    params.put(TAGS, actualRecord.tags().toArray());
    params.put(SCHEMA_VERSION, actualRecord.schemaVersion());
  }

  @Override
  public void keyParams(Map<String, Object> params, EventRecordKey key) {
    params.put(ID, key.id());
  }

  @Override
  public void queryBuilder(EventRecordQuery query, QueryBuilder builder) {
    // todo add tags and commandIds to query
    builder
      .iLike(
        new QueryFilters<>(String.class)
          .filterColumn(EVENT_CLASS)
          .filterParams(query.eventClasses())
      )
      .iLike(
        new QueryFilters<>(String.class)
          .filterColumn(AGGREGATE_ID)
          .filterParams(query.entityId())
      )
      .from(
        new QueryFilter<>(Long.class)
          .filterColumn(EVENT_VERSION)
          .filterParam(query.eventVersionFrom())
      )
      .to(
        new QueryFilter<>(Long.class)
          .filterColumn(EVENT_VERSION)
          .filterParam(query.eventVersionTo())
      )
      .from(
        new QueryFilter<>(Long.class)
          .filterColumn(ID)
          .filterParam(query.idFrom())
      )
      .to(
        new QueryFilter<>(Long.class)
          .filterColumn(ID)
          .filterParam(query.idTo())
      )
    ;
  }
}
