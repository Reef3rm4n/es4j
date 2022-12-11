package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.models.PersistedRecord;
import io.vertx.skeleton.models.RepositoryRecord;
import io.vertx.skeleton.models.MessageState;
import io.vertx.skeleton.models.Tenant;
import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.util.Map;

public record MessageRecord(
  String id,
  Instant scheduled,
  Instant expiration,
  Integer priority,
  Integer retryCounter,
  MessageState messageState,
  JsonObject payload,
  JsonObject failedProcessors,
  String verticleId,
  PersistedRecord persistedRecord
) implements RepositoryRecord<MessageRecord> {


  public static MessageRecord simpleTask(String id, Tenant tenant, Object payload) {
    return new MessageRecord(
      id,
      null,
      null,
      0,
      0,
      MessageState.CREATED,
      JsonObject.mapFrom(payload),
      null,
      null,
      PersistedRecord.newRecord(tenant)
    );
  }

  public static MessageRecord task(String id, Tenant tenant, Object payload, Instant scheduled, Instant expiration, Integer priority) {
    return new MessageRecord(
      id,
      scheduled,
      expiration,
      priority,
      0,
      MessageState.CREATED,
      JsonObject.mapFrom(payload),
      null,
      null,
      PersistedRecord.newRecord(tenant)
    );
  }

  public static MessageRecord priorityTask(String id, Tenant tenant, Object payload, Integer priority) {
    return new MessageRecord(
      id,
      null,
      null,
      priority,
      0,
      MessageState.CREATED,
      JsonObject.mapFrom(payload),
      null,
      null,
      PersistedRecord.newRecord(tenant)
    );
  }

  @Override
  public MessageRecord with(final PersistedRecord persistedRecord) {
    return new MessageRecord(id, scheduled, expiration, priority, retryCounter, messageState, payload, failedProcessors, verticleId, persistedRecord);
  }

  public MessageRecord withState(final MessageState retry) {
    return new MessageRecord(id, scheduled, expiration, priority, 0, retry, payload, failedProcessors, verticleId, persistedRecord);
  }

  public MessageRecord increaseCounter() {
    return new MessageRecord(id, scheduled, expiration, priority, retryCounter + 1, MessageState.RETRY, payload, failedProcessors, verticleId, persistedRecord);
  }

  public MessageRecord withFailures(Map<String, Throwable> failures) {
    if (!failures.isEmpty()) {
      return new MessageRecord(id, scheduled, expiration, priority, retryCounter + 1, MessageState.RETRY, payload, JsonObject.mapFrom(failures), verticleId, persistedRecord);
    }
    return new MessageRecord(id, scheduled, expiration, priority, retryCounter, MessageState.PROCESSED, payload, this.failedProcessors, verticleId, persistedRecord);
  }
}
