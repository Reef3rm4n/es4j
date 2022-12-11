package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.models.Tenant;

public record MessageID(
  String id,
  Tenant tenant
) {
}
