package io.vertx.skeleton.ccp.mappers;


import io.vertx.skeleton.ccp.models.MessageRecord;
import io.vertx.skeleton.ccp.models.MessageRecordID;
import io.vertx.skeleton.ccp.models.MessageRecordQuery;
import io.vertx.skeleton.orm.RepositoryMapper;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.sqlclient.templates.RowMapper;
import io.vertx.mutiny.sqlclient.templates.TupleMapper;
import io.vertx.skeleton.models.MessageState;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static io.vertx.skeleton.orm.Constants.*;
import static io.vertx.skeleton.orm.mappers.Constants.*;


public class MessageQueueMapper implements RepositoryMapper<MessageRecordID, MessageRecord, MessageRecordQuery> {
  private final String table;

  public MessageQueueMapper(final String table) {
    this.table = table;
  }

  public final RowMapper<MessageRecord> ROW_MAPPER = RowMapper.newInstance(
    row -> new MessageRecord(
      row.getString(MESSAGE_ID),
      row.getLocalDateTime(SCHEDULED) != null ? row.getLocalDateTime(SCHEDULED).toInstant(ZoneOffset.UTC) : null,
      row.getLocalDateTime(EXPIRATION) != null ? row.getLocalDateTime(EXPIRATION).toInstant(ZoneOffset.UTC) : null,
      row.getInteger(PRIORITY),
      row.getInteger(RETRY_COUNTER),
      MessageState.valueOf(row.getString(STATE)),
      row.getJsonObject(PAYLOAD),
      row.getJsonObject(FAILURES),
      row.getString(VERTICLE_ID),
      from(row)
    )
  );

  public static final TupleMapper<MessageRecord> TUPLE_MAPPER = TupleMapper.mapper(
    taskQueueEntry -> {
      Map<String, Object> parameters = taskQueueEntry.persistedRecord().params();
      parameters.put(MESSAGE_ID, taskQueueEntry.id());
      if (taskQueueEntry.scheduled() != null) {
        parameters.put(SCHEDULED, LocalDateTime.ofInstant(taskQueueEntry.scheduled(), ZoneOffset.UTC));
      }
      if (taskQueueEntry.expiration() != null) {
        parameters.put(EXPIRATION, LocalDateTime.ofInstant(taskQueueEntry.expiration(), ZoneOffset.UTC));
      }
      parameters.put(PRIORITY, taskQueueEntry.priority());
      parameters.put(RETRY_COUNTER, taskQueueEntry.retryCounter());
      parameters.put(STATE, taskQueueEntry.messageState().name());
      parameters.put(PAYLOAD, taskQueueEntry.payload());
      if (taskQueueEntry.failedProcessors() != null && !taskQueueEntry.failedProcessors().isEmpty()) {
        parameters.put(FAILURES, taskQueueEntry.failedProcessors());
      }
      parameters.put(VERTICLE_ID, taskQueueEntry.verticleId());
      return parameters;
    }
  );

  public static final TupleMapper<MessageRecordID> KEY_MAPPER = TupleMapper.mapper(
    key -> {
      Map<String, Object> parameters = new HashMap<>();
      parameters.put(MESSAGE_ID, key.id());
      parameters.put(TENANT, key.tenant().generateString());
      return parameters;
    }
  );

  @Override
  public Set<String> insertColumns() {
    return Set.of(MESSAGE_ID, SCHEDULED, EXPIRATION, PRIORITY, RETRY_COUNTER, STATE, PAYLOAD, FAILURES, VERTICLE_ID);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(MESSAGE_ID, TENANT);
  }


  @Override
  public String table() {
    return table;
  }

  @Override
  public Set<String> updateColumns() {
    return Set.of(RETRY_COUNTER, STATE, FAILURES);
  }

  @Override
  public RowMapper<MessageRecord> rowMapper() {
    return ROW_MAPPER;
  }

  @Override
  public List<Tuple2<String, List<?>>> queryFieldsColumn(MessageRecordQuery queryFilter) {
    final var tupleList = new ArrayList<Tuple2<String, List<?>>>();
    tupleList.add(Tuple2.of(MESSAGE_ID, queryFilter.ids()));
    tupleList.add(Tuple2.of(STATE, queryFilter.states()));
    return tupleList;
  }

  @Override
  public void queryExtraFilters(MessageRecordQuery queryFilter, StringJoiner stringJoiner) {
    if (queryFilter.scheduledFrom() != null) {
      stringJoiner.add(" scheduled >= '" + LocalDateTime.ofInstant(queryFilter.scheduledFrom(), ZoneOffset.UTC) + "'::timestamp ");
    }
    if (queryFilter.scheduledTo() != null) {
      stringJoiner.add(" scheduled <= '" + LocalDateTime.ofInstant(queryFilter.scheduledTo(), ZoneOffset.UTC) + "'::timestamp ");
    }
    if (queryFilter.retryCounterFrom() != null) {
      stringJoiner.add(" retry_counter >= " + queryFilter.retryCounterFrom() + " ");
    }
    if (queryFilter.retryCounterTo() != null) {
      stringJoiner.add(" retry_counter <= " + queryFilter.retryCounterTo() + " ");
    }
    if (queryFilter.priorityFrom() != null) {
      stringJoiner.add(" beacon_counter >= " + queryFilter.priorityFrom() + " ");
    }
    if (queryFilter.priorityTo() != null) {
      stringJoiner.add(" beacon_counter <= " + queryFilter.priorityTo() + " ");
    }
  }

  @Override
  public TupleMapper<MessageRecord> tupleMapper() {
    return TUPLE_MAPPER;
  }

  @Override
  public TupleMapper<MessageRecordID> keyMapper() {
    return KEY_MAPPER;
  }


  @Override
  public Class<MessageRecord> valueClass() {
    return MessageRecord.class;
  }

  @Override
  public Class<MessageRecordID> keyClass() {
    return MessageRecordID.class;
  }
}
