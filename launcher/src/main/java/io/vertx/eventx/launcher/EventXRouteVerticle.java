package io.vertx.eventx.launcher;

import io.activej.inject.module.ModuleBuilder;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.eventx.http.HttpRouter;

import java.util.function.Supplier;

import static io.vertx.eventx.launcher.EventXMain.MAIN_MODULES;

public class EventXRouteVerticle implements Verticle {


  @Override
  public DeploymentOptions options() {
    return new DeploymentOptions()
      .setInstances(CpuCoreSensor.availableProcessors() * 2);
  }

  @Override
  public Supplier<io.vertx.core.Verticle> supplier() {
    return () -> new HttpRouter(ModuleBuilder.create().install(MAIN_MODULES));
  }
}
