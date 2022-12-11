package io.vertx.skeleton.evs;

import io.vertx.skeleton.models.Tenant;

public record FakeEntityAggregate(
  String entityId,
  Tenant tenant
) implements EntityAggregate {
}
