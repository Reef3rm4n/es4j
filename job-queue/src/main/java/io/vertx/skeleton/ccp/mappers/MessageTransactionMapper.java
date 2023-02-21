package io.vertx.skeleton.ccp.mappers;

import io.vertx.skeleton.ccp.models.*;
import io.vertx.skeleton.sql.RecordMapper;
import io.vertx.skeleton.sql.generator.filters.QueryBuilder;
import io.vertx.skeleton.sql.models.QueryFilters;
import io.vertx.sqlclient.Row;

import java.util.Map;
import java.util.Set;


public class MessageTransactionMapper implements RecordMapper<MessageTransactionID, MessageTransaction, MessageTransactionQuery> {
  public static final MessageTransactionMapper INSTANCE = new MessageTransactionMapper();
  private static final String PROCESSOR = "processor";
  private static final String MESSAGE_ID = "message_id";
  public static final String JOB_QUEUE_TX = "job_queue_tx";
  private MessageTransactionMapper(){}

  @Override
  public String table() {
    return JOB_QUEUE_TX;
  }

  @Override
  public Set<String> columns() {
    return Set.of(PROCESSOR, MESSAGE_ID);
  }


  @Override
  public Set<String> keyColumns() {
    return Set.of(MESSAGE_ID, PROCESSOR);
  }

  @Override
  public MessageTransaction rowMapper(Row row) {
    return new MessageTransaction(
      row.getString(MESSAGE_ID),
      row.getString(PROCESSOR),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, MessageTransaction record) {
    params.put(MESSAGE_ID, record.id());
    params.put(PROCESSOR, record.processorClass());
  }

  @Override
  public void keyParams(Map<String, Object> params, MessageTransactionID key) {
    params.put(PROCESSOR, key.processorClass());
    params.put(MESSAGE_ID, key.messageId());
  }

  @Override
  public void queryBuilder(MessageTransactionQuery query, QueryBuilder builder) {
    builder
      .iLike(
        new QueryFilters<>(String.class)
          .filterColumn(PROCESSOR)
          .filterParams(query.processors())
      )
      .iLike(
        new QueryFilters<>(String.class)
          .filterColumn(MESSAGE_ID)
          .filterParams(query.ids())
      );
  }


}
