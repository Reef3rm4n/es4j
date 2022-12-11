package io.vertx.skeleton.config;

import io.vertx.skeleton.models.Tenant;
import io.vertx.core.shareddata.Shareable;

public record ConfigurationKey(
  String name,
  String tClass,
  Tenant tenant
) implements Shareable {

}
