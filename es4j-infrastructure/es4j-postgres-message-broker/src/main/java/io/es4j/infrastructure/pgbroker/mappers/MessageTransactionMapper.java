package io.es4j.infrastructure.pgbroker.mappers;

import io.es4j.infrastructure.pgbroker.models.ConsumerTransactionKey;
import io.es4j.infrastructure.pgbroker.models.ConsumerTransactionQuery;
import io.es4j.infrastructure.pgbroker.models.ConsumerTransactionRecord;
import io.es4j.sql.RecordMapper;
import io.es4j.sql.generator.filters.QueryBuilder;
import io.es4j.sql.models.QueryFilters;
import io.vertx.sqlclient.Row;

import java.util.Map;
import java.util.Set;


public class MessageTransactionMapper implements RecordMapper<ConsumerTransactionKey, ConsumerTransactionRecord, ConsumerTransactionQuery> {
  public static final MessageTransactionMapper INSTANCE = new MessageTransactionMapper();
  private static final String MESSAGE_ID = "message_id";
  private static final String CONSUMER = "consumer";
  public static final String TASK_QUEUE_TX = "message_broker_tx";

  private MessageTransactionMapper() {
  }

  @Override
  public String table() {
    return TASK_QUEUE_TX;
  }

  @Override
  public Set<String> columns() {
    return Set.of(MESSAGE_ID, CONSUMER);
  }


  @Override
  public Set<String> keyColumns() {
    return Set.of(MESSAGE_ID, CONSUMER);
  }

  @Override
  public ConsumerTransactionRecord rowMapper(Row row) {
    return new ConsumerTransactionRecord(
      row.getString(MESSAGE_ID),
      row.getString(CONSUMER),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, ConsumerTransactionRecord actualRecord) {
    params.put(MESSAGE_ID, actualRecord.id());
    params.put(CONSUMER, actualRecord.consumer());
  }

  @Override
  public void keyParams(Map<String, Object> params, ConsumerTransactionKey key) {
    params.put(MESSAGE_ID, key.messageId());
    params.put(CONSUMER, key.consumer());
  }

  @Override
  public void queryBuilder(ConsumerTransactionQuery query, QueryBuilder builder) {
    builder
      .iLike(
        new QueryFilters<>(String.class)
          .filterColumn(MESSAGE_ID)
          .filterParams(query.ids())
      )
      .iLike(
        new QueryFilters<>(String.class)
          .filterColumn(CONSUMER)
          .filterParams(query.consumers())
      );
  }


}
