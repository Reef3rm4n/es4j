package io.es4j.infrastructure.messagebroker.postgres.models;

import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.RepositoryRecord;
import io.soabase.recordbuilder.core.RecordBuilder;
import io.es4j.infrastructure.messagebroker.models.MessageState;
import io.es4j.infrastructure.messagebroker.models.RawMessage;
import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.util.Map;

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
  BaseRecord baseRecord
) implements RepositoryRecord<MessageRecord> {


  public static MessageRecord simpleTask(String id, String tenant, Object payload) {
    return new MessageRecord(
      id,
      null,
      null,
      0,
      0,
      MessageState.CREATED,
      null,
      JsonObject.mapFrom(payload),
      null,
      null,
      BaseRecord.newRecord(tenant)
    );
  }

  public static MessageRecord task(String id, String tenant, Object payload, Instant scheduled, Instant expiration, Integer priority) {
    return new MessageRecord(
      id,
      scheduled,
      expiration,
      priority,
      0,
      MessageState.CREATED,
      null,
      JsonObject.mapFrom(payload),
      null,
      null,
      BaseRecord.newRecord(tenant)
    );
  }

  public static MessageRecord priorityTask(String id, String tenant, Object payload, Integer priority) {
    return new MessageRecord(
      id,
      null,
      null,
      priority,
      0,
      MessageState.CREATED,
      null,
      JsonObject.mapFrom(payload),
      null,
      null,
      BaseRecord.newRecord(tenant)
    );
  }

  public MessageRecord withState(final MessageState retry) {
    return new MessageRecord(id, scheduled, expiration, priority, 0, retry, payloadClass, payload, failedProcessors, verticleId, baseRecord);
  }

  public MessageRecord increaseCounter() {
    return new MessageRecord(id, scheduled, expiration, priority, retryCounter + 1, MessageState.RETRY, payloadClass, payload, failedProcessors, verticleId, baseRecord);
  }

  public MessageRecord withFailures(Map<String, Throwable> failures) {
    if (!failures.isEmpty()) {
      return new MessageRecord(id, scheduled, expiration, priority, retryCounter + 1, MessageState.RETRY, payloadClass, payload, JsonObject.mapFrom(failures), verticleId, baseRecord);
    }
    return new MessageRecord(id, scheduled, expiration, priority, retryCounter, MessageState.PROCESSED, payloadClass, payload, this.failedProcessors, verticleId, baseRecord);
  }

  @Override
  public MessageRecord with(BaseRecord baseRecord) {
    return new MessageRecord(id, scheduled, expiration, priority, retryCounter, messageState, payloadClass, payload, failedProcessors, verticleId, baseRecord);
  }

  public static MessageRecord from(RawMessage rawMessage) {
    return new MessageRecord (
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
      BaseRecord.newRecord(rawMessage.tenant())
    );
  }
}
