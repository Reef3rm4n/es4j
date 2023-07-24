package io.es4j.infrastructure.pgbroker.mappers;


import io.es4j.infrastructure.pgbroker.models.MessageRecord;
import io.es4j.infrastructure.pgbroker.models.MessageRecordKey;
import io.es4j.infrastructure.pgbroker.models.MessageRecordQuery;
import io.es4j.infrastructure.pgbroker.models.MessageState;
import io.es4j.sql.RecordMapper;
import io.es4j.sql.generator.filters.QueryBuilder;
import io.es4j.sql.models.QueryFilter;
import io.es4j.sql.models.QueryFilters;
import io.vertx.sqlclient.Row;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


public class MessageMapper implements RecordMapper<MessageRecordKey, MessageRecord, MessageRecordQuery> {
  private static final String MESSAGE_ID = "message_id";
  private static final String SCHEDULED = "scheduled";
  private static final String EXPIRATION = "expiration";
  private static final String PRIORITY = "priority";
  private static final String STATE = "state";
  private static final String PAYLOAD = "payload";
  private static final String PARTITION_KEY = "partition_key";
  private static final String VERTICLE_ID = "verticle_id";
  private static final String MESSAGE_SEQUENCE = "message_sequence";
  private static final String SCHEMA_VERSION = "schema_version";
  private static final String PARTITION_ID = "partition_id";
  private static final String MESSAGE_BROKER = "message_broker";
  private static final String MESSAGE_ADDRESS = "message_address";

  public static MessageMapper INSTANCE = new MessageMapper();

  private MessageMapper() {
  }

  @Override
  public String table() {
    return MESSAGE_BROKER;
  }

  @Override
  public Set<String> columns() {
    return Set.of(MESSAGE_ID, SCHEDULED, EXPIRATION, PRIORITY, STATE, PAYLOAD, VERTICLE_ID, PARTITION_ID, PARTITION_KEY, MESSAGE_ADDRESS);
  }

  @Override
  public Set<String> updatableColumns() {
    return Set.of(STATE, VERTICLE_ID);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(MESSAGE_ID);
  }

  @Override
  public MessageRecord rowMapper(Row row) {
    return new MessageRecord(
      row.getString(MESSAGE_ID),
      row.getLocalDateTime(SCHEDULED) != null ? row.getLocalDateTime(SCHEDULED).toInstant(ZoneOffset.UTC) : null,
      row.getLocalDateTime(EXPIRATION) != null ? row.getLocalDateTime(EXPIRATION).toInstant(ZoneOffset.UTC) : null,
      row.getInteger(PRIORITY),
      MessageState.valueOf(row.getString(STATE)),
      row.getString(MESSAGE_ADDRESS),
      row.getJsonObject(PAYLOAD),
      row.getString(VERTICLE_ID),
      row.getLong(MESSAGE_SEQUENCE),
      row.getString(PARTITION_ID),
      row.getString(PARTITION_KEY),
      row.getInteger(SCHEMA_VERSION),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, MessageRecord actualRecord) {
    params.put(MESSAGE_ID, actualRecord.messageId());
    if (actualRecord.scheduled() != null) {
      params.put(SCHEDULED, LocalDateTime.ofInstant(actualRecord.scheduled(), ZoneOffset.UTC));
    }
    if (actualRecord.expiration() != null) {
      params.put(EXPIRATION, LocalDateTime.ofInstant(actualRecord.expiration(), ZoneOffset.UTC));
    }
    params.put(PRIORITY, actualRecord.priority());
    params.put(STATE, actualRecord.messageState().name());
    params.put(PAYLOAD, actualRecord.payload());
    params.put(SCHEMA_VERSION, Objects.requireNonNullElse(actualRecord.schemaVersion(),0));
    params.put(VERTICLE_ID, actualRecord.verticleId());
    params.put(MESSAGE_SEQUENCE, actualRecord.messageSequence());
    params.put(PARTITION_KEY, actualRecord.partitionKey());
    params.put(PARTITION_ID, actualRecord.partitionId());
    params.put(MESSAGE_ADDRESS, actualRecord.messageAddress());
  }

  @Override
  public void keyParams(Map<String, Object> params, MessageRecordKey key) {
    params.put(MESSAGE_ID, key.id());
  }

  @Override
  public void queryBuilder(MessageRecordQuery query, QueryBuilder builder) {
    builder.iLike(
        new QueryFilters<>(String.class)
          .filterColumn(MESSAGE_ID)
          .filterParams(query.ids())
      )
      .iLike(
        new QueryFilters<>(MessageState.class)
          .filterColumn(STATE)
          .filterParams(query.states())
      )
      .from(
        new QueryFilter<>(Instant.class)
          .filterColumn(SCHEDULED)
          .filterParam(query.scheduledFrom())
      )
      .to(
        new QueryFilter<>(Instant.class)
          .filterColumn(SCHEDULED)
          .filterParam(query.scheduledTo())
      )
      .from(
        new QueryFilter<>(Integer.class)
          .filterColumn(PRIORITY)
          .filterParam(query.priorityFrom())
      )
      .to(
        new QueryFilter<>(Integer.class)
          .filterColumn(PRIORITY)
          .filterParam(query.priorityTo())
      );
  }

}
