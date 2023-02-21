package io.vertx.skeleton.ccp.models;


import java.time.Instant;

public record Message<T>(
  String id,
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
    if (id == null) {
      throw new IllegalArgumentException("Id must not be null");
    }
  }
}
