package io.es4j.infrastructure.pgbroker.models;


import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.RepositoryRecord;
import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;

@RecordBuilder
public record ConsumerFailureRecord(
  String messageId,
  String consumer,
  JsonObject error,
  BaseRecord baseRecord
) implements RepositoryRecord<ConsumerFailureRecord> {

  @Override
  public ConsumerFailureRecord with(BaseRecord baseRecord) {
    return ConsumerFailureRecordBuilder.builder(this)
      .baseRecord(baseRecord)
      .build();
  }
}
