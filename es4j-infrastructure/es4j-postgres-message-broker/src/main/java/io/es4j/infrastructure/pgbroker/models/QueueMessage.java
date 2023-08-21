package io.es4j.infrastructure.pgbroker.models;


import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@RecordBuilder
public record QueueMessage<T>(
  T payload,
  String messageId,
  Instant scheduled,
  Instant expiration,
  Integer priority,
  Integer schemaVersion,
  String address
) {

  public QueueMessage(T payload, String messageId, Instant scheduled, Instant expiration, Integer priority, Integer schemaVersion, String address) {
    this.payload = Objects.requireNonNull(payload);
    this.messageId = Objects.requireNonNullElse(messageId,UUID.randomUUID().toString());
    this.scheduled = scheduled;
    this.expiration = expiration;
    this.priority = priority;
    this.schemaVersion = Objects.requireNonNullElse(schemaVersion,0);
    this.address = Objects.requireNonNullElse(address,payload.getClass().getName());
  }
}
