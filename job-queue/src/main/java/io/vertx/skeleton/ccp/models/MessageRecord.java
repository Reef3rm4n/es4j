package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.models.MessageState;
import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.sql.models.RecordWithoutID;
import io.vertx.skeleton.sql.models.RepositoryRecord;

import java.time.Instant;
import java.util.Map;

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
  RecordWithoutID baseRecord
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
      RecordWithoutID.newRecord(tenant)
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
      RecordWithoutID.newRecord(tenant)
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
      RecordWithoutID.newRecord(tenant)
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
  public MessageRecord with(RecordWithoutID baseRecord) {
    return new MessageRecord(id, scheduled, expiration, priority, retryCounter, messageState, payloadClass, payload, failedProcessors, verticleId, baseRecord);
  }
}
