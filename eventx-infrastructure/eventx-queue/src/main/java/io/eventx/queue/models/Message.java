package io.eventx.queue.models;


import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Instant;

@RecordBuilder
public record Message<T>(
  String messageId,
  String tenant,
  Instant scheduled,
  Instant expiration,
  Integer priority,
  T payload
) {

  public Message {
    if (priority != null && priority > 10) {
      throw new IllegalArgumentException("Max priority is 10");
    }
    if (messageId == null) {
      throw new IllegalArgumentException("Id must not be null");
    }
  }
}
