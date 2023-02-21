package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.sql.models.RecordWithoutID;
import io.vertx.skeleton.sql.models.RepositoryRecord;

public record MessageTransaction(
  String id,
  String processorClass,
  String messageClass,
  RecordWithoutID baseRecord
) implements RepositoryRecord<MessageTransaction> {
  @Override
  public MessageTransaction with(RecordWithoutID persistedRecord) {
    return new MessageTransaction(id, processorClass,  messageClass,persistedRecord);
  }
}
