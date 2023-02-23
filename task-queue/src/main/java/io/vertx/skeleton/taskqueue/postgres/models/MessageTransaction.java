package io.vertx.skeleton.taskqueue.postgres.models;

import io.vertx.skeleton.sql.models.BaseRecord;
import io.vertx.skeleton.sql.models.RepositoryRecord;

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
