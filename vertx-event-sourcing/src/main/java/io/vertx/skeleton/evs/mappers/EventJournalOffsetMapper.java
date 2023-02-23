package io.vertx.skeleton.evs.mappers;

import io.vertx.skeleton.sql.models.EmptyQuery;
import io.vertx.skeleton.evs.objects.EventJournalOffSet;
import io.vertx.skeleton.evs.objects.EventJournalOffSetKey;
import io.vertx.skeleton.sql.RecordMapper;
import io.vertx.skeleton.sql.generator.filters.QueryBuilder;
import io.vertx.sqlclient.Row;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;

import static io.vertx.skeleton.orm.mappers.Constants.*;

public class EventJournalOffsetMapper implements RecordMapper<EventJournalOffSetKey, EventJournalOffSet, EmptyQuery> {


  public static final EventJournalOffsetMapper INSTANCE = new EventJournalOffsetMapper();
  public static final String TABLE_NAME = "projection_offset";

  private EventJournalOffsetMapper() {
  }


  @Override
  public String table() {
    return TABLE_NAME;
  }

  @Override
  public Set<String> columns() {
    return Set.of(ID_OFFSET, DATE_OFFSET, CONSUMER);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(CONSUMER);
  }

  @Override
  public EventJournalOffSet rowMapper(Row row) {
    return new EventJournalOffSet(
      row.getString(CONSUMER),
      row.getLong(ID_OFFSET),
      row.getLocalDateTime(DATE_OFFSET) != null ? row.getLocalDateTime(DATE_OFFSET).toInstant(ZoneOffset.UTC) : null,
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, EventJournalOffSet actualRecord) {
    params.put(CONSUMER, actualRecord.consumer());
    params.put(ID_OFFSET, actualRecord.idOffSet());
    if (actualRecord.dateOffSet() != null) {
      params.put(DATE_OFFSET, LocalDateTime.ofInstant(actualRecord.dateOffSet(), ZoneOffset.UTC));
    }
  }

  @Override
  public void keyParams(Map<String, Object> params, EventJournalOffSetKey key) {
    params.put(CONSUMER, key.consumer());
  }

  @Override
  public void queryBuilder(EmptyQuery query, QueryBuilder builder) {

  }

}
