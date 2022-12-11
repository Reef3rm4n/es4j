package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.models.PersistedRecord;
import io.vertx.skeleton.models.RepositoryRecord;

public record MessageTransaction(
  String id,
  String processorClass,
  ProcessorType type,
  PersistedRecord persistedRecord
) implements RepositoryRecord<MessageTransaction> {
  @Override
  public MessageTransaction with(PersistedRecord persistedRecord) {
    return new MessageTransaction(id, processorClass, type, persistedRecord);
  }
}
