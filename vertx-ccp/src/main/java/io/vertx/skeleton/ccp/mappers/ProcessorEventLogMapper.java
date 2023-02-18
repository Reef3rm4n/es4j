package io.vertx.skeleton.ccp.mappers;


import io.vertx.skeleton.ccp.models.MessageRecordID;
import io.vertx.skeleton.orm.RepositoryMapper;
import io.vertx.mutiny.sqlclient.templates.RowMapper;
import io.vertx.mutiny.sqlclient.templates.TupleMapper;
import io.vertx.skeleton.models.ConcurrentTaskEvent;
import io.vertx.skeleton.models.EmptyQuery;
import io.vertx.skeleton.models.exceptions.OrmGenericException;
import io.vertx.skeleton.models.TaskEventType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.vertx.skeleton.orm.Constants.*;
import static io.vertx.skeleton.orm.mappers.Constants.*;


public class ProcessorEventLogMapper implements RepositoryMapper<MessageRecordID, ConcurrentTaskEvent, EmptyQuery> {
  private final String table;

  public ProcessorEventLogMapper(final String table) {
    this.table = table;
  }
  public  final RowMapper<ConcurrentTaskEvent> ROW_MAPPER = RowMapper.newInstance(
    row -> new ConcurrentTaskEvent(
      row.getString(ID),
      TaskEventType.valueOf(row.getString(TYPE)),
      row.getJsonObject(PAYLOAD),
      row.getJsonObject(ERROR),
      from(row)
    )
  );

  @Override
  public Set<String> insertColumns() {
    return Set.of(ID, TYPE, PAYLOAD, ERROR);
  }

  @Override
  public Set<String> updateColumns() {
    throw OrmGenericException.notImplemented();
  }

  public static final TupleMapper<ConcurrentTaskEvent> TUPLE_MAPPER = TupleMapper.mapper(
    notification -> {
      Map<String, Object> parameters = notification.persistedRecord().params();
      parameters.put(ID, notification.id());
      parameters.put(TYPE, notification.type().name());
      parameters.put(PAYLOAD, notification.payload());
      parameters.put(ERROR, notification.error());
      return parameters;
    }
  );

  public static final TupleMapper<MessageRecordID> KEY_MAPPER = TupleMapper.mapper(
    key -> {
      Map<String, Object> parameters = new HashMap<>();
      parameters.put(ID, key.id());
      parameters.put(TENANT, key.tenant().generateString());
      return parameters;
    }
  );

  @Override
  public Set<String> keyColumns() {
    return Set.of(ID,TENANT);
  }


  @Override
  public String table() {
    return table;
  }


  @Override
  public RowMapper<ConcurrentTaskEvent> rowMapper() {
    return ROW_MAPPER;
  }

  @Override
  public TupleMapper<ConcurrentTaskEvent> tupleMapper() {
    return TUPLE_MAPPER;
  }

  @Override
  public TupleMapper<MessageRecordID> keyMapper() {
    return KEY_MAPPER;
  }


  @Override
  public Class<ConcurrentTaskEvent> valueClass() {
    return ConcurrentTaskEvent.class;
  }

  @Override
  public Class<MessageRecordID> keyClass() {
    return MessageRecordID.class;
  }
}
