package io.vertx.skeleton.framework;

import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;

import java.util.function.Supplier;

public abstract class Verticle extends AbstractVerticle {


  public abstract DeploymentOptions options();

  public abstract Supplier<io.vertx.core.Verticle> supplier();
}
