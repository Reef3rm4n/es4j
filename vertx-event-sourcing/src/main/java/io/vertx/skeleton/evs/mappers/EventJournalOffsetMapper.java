package io.vertx.skeleton.evs.mappers;

import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.models.EmptyQuery;
import io.vertx.skeleton.evs.objects.EventJournalOffSet;
import io.vertx.skeleton.evs.objects.EventJournalOffSetKey;
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


  private String tableName;

  public <T extends EntityAggregate> EventJournalOffsetMapper(Class<T> entityAggregateClass) {
    this.tableName = entityAggregateClass.getSimpleName() + "_consumers_offset";
  }

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
    return tableName;
  }

  @Override
  public Set<String> insertColumns() {
    return Set.of( ID_OFFSET, DATE_OFFSET, CONSUMER);
  }

  @Override
  public Set<String> updateColumns() {
    return Set.of(ID_OFFSET, DATE_OFFSET);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(CONSUMER);
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
