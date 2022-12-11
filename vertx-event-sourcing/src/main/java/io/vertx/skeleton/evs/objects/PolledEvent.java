package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.models.Tenant;

public record PolledEvent(
  String entityId,
  Tenant tenant,
  Object event
) {
}
