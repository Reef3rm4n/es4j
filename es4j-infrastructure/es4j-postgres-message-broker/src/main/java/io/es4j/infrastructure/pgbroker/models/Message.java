package io.es4j.infrastructure.pgbroker.models;


import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record Message<T>(
  MessageHeaders headers,
  T payload
) {

  public Message(T payload) {
    this(MessageHeaders.topicMessage(payload.getClass().getName()), payload);
  }

  public Message(T payload, String topic) {
    this(MessageHeaders.topicMessage(topic), payload);
  }
}
