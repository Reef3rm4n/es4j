package io.vertx.skeleton.taskqueue.models;

import io.vertx.core.json.JsonObject;
import io.vertx.skeleton.models.MessageState;
import java.time.Instant;

public record RawMessage(
  String id,
  Instant scheduled,
  Instant expiration,
  Integer priority,
  Integer retryCounter,
  MessageState messageState,
  String payloadClass,
  JsonObject payload,
  JsonObject failures,
  String tenant
)  {

  public RawMessage withState(MessageState messageState) {
    return new RawMessage(
      id,
      scheduled,
      expiration,
      priority,
      retryCounter,
      messageState,
      payloadClass,
      payload,
      failures,
      tenant
    );
  }
}
