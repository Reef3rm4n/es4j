package io.es4j.infrastructure.pgbroker.models;


import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.RepositoryRecord;
import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;

import java.time.Instant;

import static java.util.Objects.requireNonNullElse;

@RecordBuilder
public record MessageRecord(
  String messageId,
  Instant scheduled,
  Instant expiration,
  Integer priority,
  MessageState messageState,
  String messageAddress,
  JsonObject payload,
  String verticleId,
  Long messageSequence,
  String partitionId,
  String partitionKey,
  Integer schemaVersion,
  BaseRecord baseRecord
) implements RepositoryRecord<MessageRecord> {


  @Override
  public MessageRecord with(BaseRecord baseRecord) {
    return MessageRecordBuilder.builder(this)
      .baseRecord(baseRecord)
      .build();
  }

  public static MessageRecord from(RawMessage rawMessage) {
    return new MessageRecord(
      rawMessage.messageId(),
      rawMessage.scheduled(),
      rawMessage.expiration(),
      rawMessage.priority(),
      rawMessage.messageState(),
      rawMessage.messageAddress(),
      rawMessage.payload(),
      null,
      rawMessage.messageSequence(),
      rawMessage.partitionId(),
      rawMessage.partitionKey(),
      rawMessage.schemaVersion(),
      BaseRecord.newRecord(requireNonNullElse(rawMessage.tenant(), "default"))
    );
  }
}
