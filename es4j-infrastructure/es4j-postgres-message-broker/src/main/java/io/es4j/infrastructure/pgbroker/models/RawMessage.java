package io.es4j.infrastructure.pgbroker.models;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;

import java.time.Instant;

@RecordBuilder
public record RawMessage(
  String messageId,
  Instant scheduled,
  Instant expiration,
  Integer priority,
  MessageState messageState,
  String messageAddress,
  JsonObject payload,
  String tenant,
  Long messageSequence,
  String partitionId,
  String partitionKey,
  Integer schemaVersion
) {

  public RawMessage withState(MessageState messageState) {
    return RawMessageBuilder.builder(this)
      .messageState(messageState)
      .build();
  }


}
