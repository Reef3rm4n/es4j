package io.vertx.skeleton.taskqueue.postgres.models;

import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.models.MessageState;
import io.vertx.skeleton.sql.models.BaseRecord;
import io.vertx.skeleton.sql.models.RepositoryRecord;

import java.time.Instant;
import java.util.Map;

public record DeadLetterRecord(
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
) implements RepositoryRecord<DeadLetterRecord> {


  public static DeadLetterRecord simpleTask(String id, String tenant, Object payload) {
    return new DeadLetterRecord(
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

  public static DeadLetterRecord task(String id, String tenant, Object payload, Instant scheduled, Instant expiration, Integer priority) {
    return new DeadLetterRecord(
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

  public static DeadLetterRecord priorityTask(String id, String tenant, Object payload, Integer priority) {
    return new DeadLetterRecord(
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

  public DeadLetterRecord withState(final MessageState retry) {
    return new DeadLetterRecord(id, scheduled, expiration, priority, 0, retry, payloadClass, payload, failedProcessors, verticleId, baseRecord);
  }

  public DeadLetterRecord increaseCounter() {
    return new DeadLetterRecord(id, scheduled, expiration, priority, retryCounter + 1, MessageState.RETRY, payloadClass, payload, failedProcessors, verticleId, baseRecord);
  }

  public DeadLetterRecord withFailures(Map<String, Throwable> failures) {
    if (!failures.isEmpty()) {
      return new DeadLetterRecord(id, scheduled, expiration, priority, retryCounter + 1, MessageState.RETRY, payloadClass, payload, JsonObject.mapFrom(failures), verticleId, baseRecord);
    }
    return new DeadLetterRecord(id, scheduled, expiration, priority, retryCounter, MessageState.PROCESSED, payloadClass, payload, this.failedProcessors, verticleId, baseRecord);
  }

  @Override
  public DeadLetterRecord with(BaseRecord baseRecord) {
    return new DeadLetterRecord(id, scheduled, expiration, priority, retryCounter, messageState, payloadClass, payload, failedProcessors, verticleId, baseRecord);
  }
}
