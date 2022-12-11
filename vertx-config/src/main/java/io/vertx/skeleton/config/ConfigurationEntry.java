package io.vertx.skeleton.config;

import io.vertx.skeleton.models.Tenant;
import io.vertx.core.shareddata.Shareable;

public interface ConfigurationEntry extends Shareable {

  Tenant tenant();
  String name();
}
