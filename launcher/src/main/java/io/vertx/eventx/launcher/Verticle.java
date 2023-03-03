package io.vertx.eventx.launcher;

import io.vertx.core.DeploymentOptions;

import java.util.function.Supplier;

public interface Verticle  {


  DeploymentOptions options();

  Supplier<io.vertx.core.Verticle> supplier();
}
