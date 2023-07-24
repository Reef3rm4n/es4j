package io.es4j.infrastructure.pgbroker.models;


import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.RepositoryRecord;
import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record ConsumerTransactionRecord(
  String id,
  String consumer,
  BaseRecord baseRecord
) implements RepositoryRecord<ConsumerTransactionRecord> {

  @Override
  public ConsumerTransactionRecord with(BaseRecord baseRecord) {
    return ConsumerTransactionRecordBuilder.builder(this).baseRecord(baseRecord).build();
  }
}
