package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.models.Tenant;

public record EntityEventKey(
  String entityId,
  Long eventVersion,
  Tenant tenant
) {
}
