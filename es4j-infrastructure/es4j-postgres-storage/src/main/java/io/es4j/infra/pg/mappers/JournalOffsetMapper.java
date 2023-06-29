package io.es4j.infra.pg.mappers;

import io.es4j.infra.pg.models.EventJournalOffSet;
import io.es4j.infra.pg.models.EventJournalOffSetKey;
import io.es4j.sql.RecordMapper;
import io.es4j.sql.generator.filters.QueryBuilder;
import io.es4j.sql.models.EmptyQuery;
import io.vertx.sqlclient.Row;

import java.util.Map;
import java.util.Set;


public class JournalOffsetMapper implements RecordMapper<EventJournalOffSetKey, EventJournalOffSet, EmptyQuery> {


  public static final JournalOffsetMapper INSTANCE = new JournalOffsetMapper();
  public static final String TABLE_NAME = "offset_store";
  public static final String ID_OFFSET = "id_offset";
  public static final String EVENT_OFFSET = "event_offset";
  public static final String CONSUMER = "consumer";
  public static final String DATE_OFFSET = "data_offset";



  private JournalOffsetMapper() {
  }


  @Override
  public String table() {
    return TABLE_NAME;
  }

  @Override
  public Set<String> columns() {
    return Set.of(ID_OFFSET,EVENT_OFFSET, CONSUMER);
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
      row.getLong(EVENT_OFFSET),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, EventJournalOffSet actualRecord) {
    params.put(CONSUMER, actualRecord.consumer());
    params.put(ID_OFFSET, actualRecord.idOffSet());
    params.put(EVENT_OFFSET, actualRecord.eventVersionOffset());
  }

  @Override
  public void keyParams(Map<String, Object> params, EventJournalOffSetKey key) {
    params.put(CONSUMER, key.consumer());
  }

  @Override
  public void queryBuilder(EmptyQuery query, QueryBuilder builder) {

  }

}
