package io.es4j.infrastructure.pgbroker.models;


import io.soabase.recordbuilder.core.RecordBuilder;

import java.util.Objects;
import java.util.UUID;

@RecordBuilder
public record TopicMessage<T>(
  T payload,
  String messageId,
  String partitionKey,
  Integer schemaVersion,
  String address
) {

  public TopicMessage(T payload, String messageId, String partitionKey, Integer schemaVersion, String address) {
    this.payload = Objects.requireNonNull(payload);
    this.messageId = Objects.requireNonNullElse(messageId, UUID.randomUUID().toString());
    this.partitionKey = Objects.requireNonNullElse(partitionKey, payload.getClass().getName());
    this.schemaVersion = Objects.requireNonNullElse(schemaVersion, 0);
    this.address = Objects.requireNonNullElse(address, payload.getClass().getName());
  }
}
