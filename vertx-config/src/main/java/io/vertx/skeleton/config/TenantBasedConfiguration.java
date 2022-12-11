package io.vertx.skeleton.config;

import io.vertx.skeleton.models.Tenant;
import io.vertx.core.json.JsonObject;

public record TenantBasedConfiguration(
  Tenant tenant,
  JsonObject configuration
) {
}
