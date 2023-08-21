package io.es4j.infrastructure.pgbroker.models;

import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@RecordBuilder
public record MessageHeaders(
  String messageId,
  String tenant,
  String partitionKey,
  Instant scheduled,
  Instant expiration,
  Integer priority,
  Integer schemaVersion,
  String address,
  Map<String, Object> metadata
) {

  public static final String NONE = "none";

  public MessageHeaders {
    Objects.requireNonNull(address, "address must not be null");
    if (priority != null && priority > 10) {
      throw new IllegalArgumentException("Max priority is 10");
    }
    if (messageId == null) {
      throw new IllegalArgumentException("Id must not be null");
    }
    Objects.requireNonNull(messageId, "message messageId must not be null");
    Objects.requireNonNull(tenant, "tenant messageId must not be null");
    Objects.requireNonNull(partitionKey, "partitionKey messageId must not be null");
    if (!partitionKey.equals(NONE)) {
      if (Objects.nonNull(priority)) {
        throw new IllegalArgumentException("priority cannot be set for partitioned messages");
      }
      if (Objects.nonNull(expiration)) {
        throw new IllegalArgumentException("expiration cannot be set for partitioned messages");
      }
      if (Objects.nonNull(scheduled)) {
        throw new IllegalArgumentException("scheduling cannot be set for partitioned messages");
      }
    }
    Objects.requireNonNull(schemaVersion, "schema version cannot be null");
  }
  public static MessageHeaders queueMessage(String address) {
    return new MessageHeaders(
      UUID.randomUUID().toString(),
      "default",
      NONE,
      null,
      null,
      null,
      0,
      address,
      null
    );
  }
  public static MessageHeaders queueMessage(String messageId, String address) {
    return new MessageHeaders(
      messageId,
      "default",
      NONE,
      null,
      null,
      null,
      0,
      address,
      null
    );
  }

  public static MessageHeaders queueMessage(String messageId, String topic, String tenant) {
    return new MessageHeaders(
      messageId,
      tenant,
      NONE,
      null,
      null,
      null,
      0,
      topic,
      null
    );
  }

  public static MessageHeaders topicMessage(String address) {
    return new MessageHeaders(
      UUID.randomUUID().toString(),
      "default",
      NONE,
      null,
      null,
      null,
      0,
      address,
      null
    );
  }

  public static MessageHeaders topicMessage(String partitionKey, String topic) {
    return new MessageHeaders(
      UUID.randomUUID().toString(),
      "default",
      Objects.requireNonNull(partitionKey),
      null,
      null,
      null,
      0,
      topic,
      null
    );
  }

  public static MessageHeaders topicMessage(String partitionKey, String topic, String messageId) {
    return new MessageHeaders(
      messageId,
      "default",
      Objects.requireNonNull(partitionKey),
      null,
      null,
      null,
      0,
      topic,
      null
    );
  }

  public static MessageHeaders topicMessage(String partitionKey, String messageAddress, String id, String tenant) {
    return new MessageHeaders(
      id,
      tenant,
      Objects.requireNonNull(partitionKey),
      null,
      null,
      null,
      0,
      messageAddress,
      null
    );
  }
}
