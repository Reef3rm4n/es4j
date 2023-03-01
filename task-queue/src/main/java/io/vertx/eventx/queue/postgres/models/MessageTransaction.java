package io.vertx.eventx.queue.postgres.models;

import io.vertx.eventx.sql.models.BaseRecord;
import io.vertx.eventx.sql.models.RepositoryRecord;

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
