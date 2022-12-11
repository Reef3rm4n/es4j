package io.vertx.skeleton.models;

public record EntityEventKey(
  String entityId,
  Long eventVersion,
  Tenant tenant
) {
}
