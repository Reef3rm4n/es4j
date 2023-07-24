package io.es4j.infrastructure.pgbroker.mappers;


import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.sql.RecordMapper;
import io.es4j.sql.generator.filters.QueryBuilder;
import io.es4j.sql.models.QueryFilters;
import io.vertx.sqlclient.Row;

import java.util.Map;
import java.util.Set;


public class ConsumerFailureMapper implements RecordMapper<ConsumerFailureKey, ConsumerFailureRecord, ConsumerFailureQuery> {
  private static final String MESSAGE_ID = "message_id";
  private static final String CONSUMER = "consumer";
  private static final String ERROR = "error";
  private static final String CONSUMER_FAILURE = "message_broker_consumer_failure";

  public static final ConsumerFailureMapper INSTANCE = new ConsumerFailureMapper();

  private ConsumerFailureMapper() {
  }

  @Override
  public String table() {
    return CONSUMER_FAILURE;
  }

  @Override
  public Set<String> columns() {
    return Set.of(MESSAGE_ID, CONSUMER, ERROR);

  }


  @Override
  public Set<String> keyColumns() {
    return Set.of(MESSAGE_ID, CONSUMER);
  }

  @Override
  public ConsumerFailureRecord rowMapper(Row row) {
    return new ConsumerFailureRecord(
      row.getString(MESSAGE_ID),
      row.getString(CONSUMER),
      row.getJsonObject(ERROR),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, ConsumerFailureRecord actualRecord) {
    params.put(MESSAGE_ID, actualRecord.messageId());
    params.put(CONSUMER, actualRecord.consumer());
    params.put(ERROR, actualRecord.error());
  }

  @Override
  public void keyParams(Map<String, Object> params, ConsumerFailureKey key) {
    params.put(MESSAGE_ID, key.messageId());
    params.put(CONSUMER, key.consumer());
  }

  @Override
  public void queryBuilder(ConsumerFailureQuery query, QueryBuilder builder) {
    builder.iLike(
        new QueryFilters<>(String.class)
          .filterColumn(MESSAGE_ID)
          .filterParams(query.ids())
      )
      .iLike(new QueryFilters<>(String.class)
        .filterColumn(CONSUMER)
        .filterParams(query.consumers())
      );
  }

}
