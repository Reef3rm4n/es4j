package io.vertx.skeleton.models;

import io.vertx.core.shareddata.Shareable;

public record EntityAggregateKey(
  String entityId,
  Tenant tenant
) implements Shareable {
}
