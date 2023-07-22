package io.es4j.infrastructure.pgbroker.models;


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

  public MessageTransaction(String id, String processorClass, String messageClass, BaseRecord baseRecord) {
    this.id = id;
    this.processorClass = processorClass;
    this.messageClass = messageClass;
    this.baseRecord = baseRecord;
  }

  @Override
  public MessageTransaction with(BaseRecord baseRecord) {
    return new MessageTransaction(id, processorClass, messageClass, baseRecord);
  }
}
