package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.models.Tenant;

public record MessageRecordID(
  String id,
  Tenant tenant
) {
}
