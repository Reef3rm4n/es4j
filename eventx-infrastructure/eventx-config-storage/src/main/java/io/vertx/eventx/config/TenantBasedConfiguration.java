package io.vertx.eventx.config;

import io.vertx.core.json.JsonObject;

public record TenantBasedConfiguration(
  String tenant,
  JsonObject configuration
) {
}
