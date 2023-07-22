package io.es4j.infrastructure.pgbroker.models;


import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.RepositoryRecord;
import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.util.Map;

import static java.util.Objects.requireNonNullElse;

@RecordBuilder
public record MessageRecord(
  String id,
  Instant scheduled,
  Instant expiration,
  Integer priority,
  Integer retryCounter,
  MessageState messageState,
  String payloadClass,
  JsonObject payload,
  JsonObject failedProcessors,
  String verticleId,
  Long messageSequence,
  String partitionId,
  String partitionKey,
  BaseRecord baseRecord
) implements RepositoryRecord<MessageRecord> {

  public MessageRecord withState(final MessageState retry) {
    return new MessageRecord(id, scheduled, expiration, priority, 0, retry, payloadClass, payload, failedProcessors, verticleId, messageSequence, partitionId, partitionKey, baseRecord);
  }

  public MessageRecord increaseCounter() {
    return new MessageRecord(id, scheduled, expiration, priority, retryCounter + 1, MessageState.RETRY, payloadClass, payload, failedProcessors, verticleId, messageSequence, partitionId, partitionKey, baseRecord);
  }

  public MessageRecord withFailures(Map<String, Throwable> failures) {
    if (!failures.isEmpty()) {
      return new MessageRecord(id, scheduled, expiration, priority, retryCounter + 1, MessageState.RETRY, payloadClass, payload, JsonObject.mapFrom(failures), verticleId, messageSequence, partitionId, partitionKey, baseRecord);
    }
    return new MessageRecord(id, scheduled, expiration, priority, retryCounter, MessageState.PROCESSED, payloadClass, payload, this.failedProcessors, verticleId, messageSequence, partitionId, partitionKey, baseRecord);
  }

  @Override
  public MessageRecord with(BaseRecord baseRecord) {
    return new MessageRecord(
      id,
      scheduled,
      expiration, priority, retryCounter, messageState, payloadClass, payload, failedProcessors, verticleId, messageSequence, partitionId, partitionKey, baseRecord);
  }

  public static MessageRecord from(RawMessage rawMessage) {
    return new MessageRecord(
      rawMessage.id(),
      rawMessage.scheduled(),
      rawMessage.expiration(),
      rawMessage.priority(),
      rawMessage.retryCounter(),
      rawMessage.messageState(),
      rawMessage.payloadClass(),
      rawMessage.payload(),
      rawMessage.failures(),
      null,
      rawMessage.messageSequence(),
      rawMessage.partitionId(),
      rawMessage.partitionKey(),
      BaseRecord.newRecord(requireNonNullElse(rawMessage.tenant(), "default"))
    );
  }
}
