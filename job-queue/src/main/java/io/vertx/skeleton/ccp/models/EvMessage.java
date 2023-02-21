package io.vertx.skeleton.ccp.models;


import java.time.Instant;
import java.util.Map;

public record EvMessage(
  String id,
  String payloadClass,
  String tenant,
  Instant scheduled,
  Instant expiration,
  Integer priority,
  Map<String, Object> payload
) implements java.io.Serializable {
}
