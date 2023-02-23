package io.vertx.skeleton.taskqueue.postgres.mappers;


import io.vertx.skeleton.taskqueue.postgres.models.DeadLetterKey;
import io.vertx.skeleton.taskqueue.postgres.models.DeadLetterRecord;
import io.vertx.skeleton.taskqueue.postgres.models.MessageRecordQuery;
import io.vertx.skeleton.models.MessageState;
import io.vertx.skeleton.sql.RecordMapper;
import io.vertx.skeleton.sql.generator.filters.QueryBuilder;
import io.vertx.skeleton.sql.models.QueryFilter;
import io.vertx.skeleton.sql.models.QueryFilters;
import io.vertx.sqlclient.Row;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;


public class DeadLetterMapper implements RecordMapper<DeadLetterKey, DeadLetterRecord, MessageRecordQuery> {
    private static final String MESSAGE_ID = "message_id";
  private static final String SCHEDULED = "scheduled";
  private static final String EXPIRATION = "expiration";
  private static final String PRIORITY = "priority";
  private static final String RETRY_COUNTER = "retry_counter";
  private static final String STATE = "state";
  private static final String PAYLOAD = "payload";
  private static final String FAILURES = "failure";
  private static final String VERTICLE_ID = "verticle_id";
  private static final String PAYLOAD_CLASS = "payload_class";
  public static final String JOB_QUEUE = "task_queue_dead_letter";

  public static DeadLetterMapper INSTANCE = new DeadLetterMapper();
  private DeadLetterMapper(){}

  @Override
  public String table() {
    return JOB_QUEUE;
  }

  @Override
  public Set<String> columns() {
    return Set.of(MESSAGE_ID, SCHEDULED, EXPIRATION, PRIORITY, RETRY_COUNTER, STATE, PAYLOAD, FAILURES, VERTICLE_ID);

  }


  @Override
  public Set<String> keyColumns() {
    return Set.of(MESSAGE_ID);
  }

  @Override
  public DeadLetterRecord rowMapper(Row row) {
    return new DeadLetterRecord(
      row.getString(MESSAGE_ID),
      row.getLocalDateTime(SCHEDULED) != null ? row.getLocalDateTime(SCHEDULED).toInstant(ZoneOffset.UTC) : null,
      row.getLocalDateTime(EXPIRATION) != null ? row.getLocalDateTime(EXPIRATION).toInstant(ZoneOffset.UTC) : null,
      row.getInteger(PRIORITY),
      row.getInteger(RETRY_COUNTER),
      MessageState.valueOf(row.getString(STATE)),
      row.getString(PAYLOAD_CLASS),
      row.getJsonObject(PAYLOAD),
      row.getJsonObject(FAILURES),
      row.getString(VERTICLE_ID),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, DeadLetterRecord actualRecord) {
    params.put(MESSAGE_ID, actualRecord.id());
    if (actualRecord.scheduled() != null) {
      params.put(SCHEDULED, LocalDateTime.ofInstant(actualRecord.scheduled(), ZoneOffset.UTC));
    }
    if (actualRecord.expiration() != null) {
      params.put(EXPIRATION, LocalDateTime.ofInstant(actualRecord.expiration(), ZoneOffset.UTC));
    }
    params.put(PRIORITY, actualRecord.priority());
    params.put(RETRY_COUNTER, actualRecord.retryCounter());
    params.put(STATE, actualRecord.messageState().name());
    params.put(PAYLOAD, actualRecord.payload());
    params.put(PAYLOAD_CLASS, actualRecord.payloadClass());
    if (actualRecord.failedProcessors() != null && !actualRecord.failedProcessors().isEmpty()) {
      params.put(FAILURES, actualRecord.failedProcessors());
    }
    params.put(VERTICLE_ID, actualRecord.verticleId());
  }

  @Override
  public void keyParams(Map<String, Object> params, DeadLetterKey key) {
    params.put(MESSAGE_ID, key.messageID());
  }

  @Override
  public void queryBuilder(MessageRecordQuery query, QueryBuilder builder) {
    builder.iLike(
        new QueryFilters<>(String.class)
          .filterColumn(MESSAGE_ID)
          .filterParams(query.ids())
      )
      .iLike(
        new QueryFilters<>(String.class)
        .filterColumn(STATE)
        .filterParams(query.states().stream().map(Enum::name).toList())
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
          .filterColumn(RETRY_COUNTER)
          .filterParam(query.retryCounterFrom())
      )
      .to(
        new QueryFilter<>(Integer.class)
          .filterColumn(RETRY_COUNTER)
          .filterParam(query.retryCounterTo())
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
