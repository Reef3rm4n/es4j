package io.vertx.skeleton.evs.objects;

import io.vertx.core.shareddata.Shareable;
import io.vertx.skeleton.models.Tenant;

public record EntityAggregateKey(
  String entityId,
  Tenant tenant
) implements Shareable {
}
