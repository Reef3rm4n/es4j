package io.vertx.skeleton.orm.mappers;

import io.vertx.skeleton.models.EmptyQuery;
import io.vertx.skeleton.models.EventJournalOffSet;
import io.vertx.skeleton.models.EventJournalOffSetKey;
import io.vertx.skeleton.orm.RepositoryMapper;
import io.vertx.mutiny.sqlclient.templates.RowMapper;
import io.vertx.mutiny.sqlclient.templates.TupleMapper;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.vertx.skeleton.orm.mappers.Constants.*;

public class EventJournalOffsetMapper implements RepositoryMapper<EventJournalOffSetKey, EventJournalOffSet, EmptyQuery> {

  public static final EventJournalOffsetMapper EVENT_JOURNAL_OFFSET_MAPPER = new EventJournalOffsetMapper();

  private EventJournalOffsetMapper(){}
  public final RowMapper<EventJournalOffSet> ROW_MAPPER = RowMapper.newInstance(
    row -> new EventJournalOffSet(
      row.getString(CONSUMER),
      row.getLong(ID_OFFSET),
      row.getLocalDateTime(DATE_OFFSET) != null ? row.getLocalDateTime(DATE_OFFSET).toInstant(ZoneOffset.UTC) : null,
      tenantLessFrom(row)
    )
  );

  public static final TupleMapper<EventJournalOffSet> TUPLE_MAPPER = TupleMapper.mapper(
    config -> {
      Map<String, Object> parameters = config.persistedRecord().tenantLessParams();
      parameters.put(CONSUMER, config.consumer());
      parameters.put(ID_OFFSET, config.idOffSet());
      if (config.dateOffSet() != null) {
        parameters.put(DATE_OFFSET, LocalDateTime.ofInstant(config.dateOffSet(), ZoneOffset.UTC));
      }
      return parameters;
    }
  );

  public static final TupleMapper<EventJournalOffSetKey> KEY_MAPPER = TupleMapper.mapper(
    savedConfigurationKey -> {
      Map<String, Object> parameters = new HashMap<>();
      parameters.put(CONSUMER, savedConfigurationKey.consumer());
      return parameters;
    }
  );


  @Override
  public String table() {
    return "event_journal_offset";
  }

  @Override
  public Set<String> insertColumns() {
    return Set.of(JOURNAL, ID_OFFSET, DATE_OFFSET, CONSUMER);
  }

  @Override
  public Set<String> updateColumns() {
    return Set.of(ID_OFFSET, DATE_OFFSET);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(CONSUMER, JOURNAL);
  }

  @Override
  public RowMapper<EventJournalOffSet> rowMapper() {
    return ROW_MAPPER;
  }

  @Override
  public TupleMapper<EventJournalOffSet> tupleMapper() {
    return TUPLE_MAPPER;
  }

  @Override
  public TupleMapper<EventJournalOffSetKey> keyMapper() {
    return KEY_MAPPER;
  }

  @Override
  public Class<EventJournalOffSet> valueClass() {
    return EventJournalOffSet.class;
  }

  @Override
  public Class<EventJournalOffSetKey> keyClass() {
    return EventJournalOffSetKey.class;
  }
}
