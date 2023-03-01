package io.vertx.eventx.storage.pg.mappers;

import io.vertx.eventx.sql.RecordMapper;
import io.vertx.eventx.sql.generator.filters.QueryBuilder;
import io.vertx.eventx.sql.models.QueryFilter;
import io.vertx.eventx.sql.models.QueryFilters;
import io.vertx.eventx.storage.pg.models.EventRecord;
import io.vertx.eventx.storage.pg.models.EventRecordKey;
import io.vertx.eventx.storage.pg.models.EventRecordQuery;
import io.vertx.sqlclient.Row;

import java.util.*;

public class EventJournalMapper implements RecordMapper<EventRecordKey, EventRecord, EventRecordQuery> {
  public static final String EVENT_JOURNAL = "event_journal";
  private static final String ENTITY_ID = "entity_id";
  private static final String ID = "id";
  private static final String EVENT = "event";
  private static final String EVENT_VERSION = "event_version";
  private static final String EVENT_CLASS = "event_class";
  private static final String COMMAND = "command";
  private static final String COMMAND_CLASS = "command_class";

  public static final EventJournalMapper INSTANCE = new EventJournalMapper();
  private EventJournalMapper(){}

  @Override
  public String table() {
    return EVENT_JOURNAL;
  }

  @Override
  public Set<String> columns() {
    return Set.of(ENTITY_ID, EVENT, EVENT_VERSION, EVENT_CLASS, COMMAND, COMMAND_CLASS);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(ENTITY_ID, EVENT_VERSION);
  }

  @Override
  public EventRecord rowMapper(Row row) {
    return new EventRecord(
      row.getLong(ID),
      row.getString(ENTITY_ID),
      row.getString(EVENT_CLASS),
      row.getLong(EVENT_VERSION),
      row.getJsonObject(EVENT),
      row.getJsonObject(COMMAND),
      row.getString(COMMAND_CLASS),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, EventRecord actualRecord) {
    params.put(ENTITY_ID, actualRecord.entityId());
    params.put(EVENT_CLASS, actualRecord.eventClass());
    params.put(EVENT_VERSION, actualRecord.eventVersion());
    params.put(EVENT, actualRecord.event());
    params.put(COMMAND, actualRecord.command());
    params.put(COMMAND_CLASS, actualRecord.commandClass());
  }

  @Override
  public void keyParams(Map<String, Object> params, EventRecordKey key) {
    params.put(ENTITY_ID, key.entityId());
    params.put(EVENT_VERSION, key.eventVersion());
  }

  @Override
  public void queryBuilder(EventRecordQuery query, QueryBuilder builder) {
    builder
      .iLike(
        new QueryFilters<>(String.class)
          .filterColumn(EVENT_CLASS)
          .filterParams(query.eventClasses())
      )
      .iLike(
        new QueryFilters<>(String.class)
          .filterColumn(ENTITY_ID)
          .filterParams(query.entityId())
      )
      .iLike(
        new QueryFilters<>(String.class)
          .filterColumn(COMMAND_CLASS)
          .filterParams(query.commandClasses())
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
