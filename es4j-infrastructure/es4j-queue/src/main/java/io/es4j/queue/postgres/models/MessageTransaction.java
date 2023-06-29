package io.es4j.queue.postgres.models;

import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.RepositoryRecord;
import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record MessageTransaction(
  String id,
  String processorClass,
  String messageClass,
  BaseRecord baseRecord
) implements RepositoryRecord<MessageTransaction> {
  @Override
  public MessageTransaction with(BaseRecord persistedRecord) {
    return new MessageTransaction(id, processorClass,  messageClass,persistedRecord);
  }
}
