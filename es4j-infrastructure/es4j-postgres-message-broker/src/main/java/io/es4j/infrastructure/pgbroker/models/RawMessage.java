package io.es4j.infrastructure.pgbroker.models;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;

import java.time.Instant;

@RecordBuilder
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
  String tenant,
  Long messageSequence,
  String partitionId,
  String partitionKey
) {

  public RawMessage withState(MessageState messageState) {
    return RawMessageBuilder.builder(this)
      .messageState(messageState)
      .build();
  }

  public RawMessage withFailure(MessageState messageState, Throwable throwable) {
    return RawMessageBuilder.builder(this)
      .retryCounter(retryCounter + 1)
      .messageState(messageState)
      .failures(
        new JsonObject()
          .put("throwable", throwable.toString())
          .put("cause", throwable.getCause() != null ? throwable.getCause().toString() : null)
      )
      .build();
  }

  public RawMessage copyStateAndFailures(RawMessage source) {
    return RawMessageBuilder.builder(this)
      .retryCounter(retryCounter + 1)
      .messageState(source.messageState)
      .failures(source.failures)
      .build();
  }

  public RawMessage park() {
    return withState(MessageState.PARKED);
  }
}
