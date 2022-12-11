package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.models.Tenant;

import java.time.Instant;
import java.util.Map;

public record EvMessage(
  String id,
  Tenant tenant,
  Instant scheduled,
  Instant expiration,
  Integer priority,
  Map<String, Object> payload
) implements java.io.Serializable {
}
