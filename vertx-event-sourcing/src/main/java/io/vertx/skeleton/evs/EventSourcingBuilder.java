package io.vertx.skeleton.evs;

import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.skeleton.evs.actors.EntityActor;

import java.util.ArrayList;
import java.util.Collection;

import static io.vertx.skeleton.evs.actors.Channel.registerActor;

public class EventSourcingBuilder<T extends Entity> {
  private final Class<T> entityAggregateClass;
  private Collection<Module> modules;
  private Vertx vertx;
  private JsonObject vertxConfiguration;


  public EventSourcingBuilder(Class<T> tClass) {
    entityAggregateClass = tClass;
  }
  public EventSourcingBuilder<T> setVertxConfiguration(JsonObject vertxConfiguration) {
    this.vertxConfiguration = vertxConfiguration;
    return this;
  }
  public EventSourcingBuilder<T> setVertx(Vertx vertx) {
    this.vertx = vertx;
    return this;
  }

  public EventSourcingBuilder<T> addModules(Module module) {
    if (modules == null) {
      this.modules = new ArrayList<>();
    }
    this.modules.add(module);
    return this;
  }
  public EventSourcingBuilder<T> setModules(Collection<Module> module) {
    this.modules = module;
    return this;
  }

  public Uni<Void> deploy(String deploymentId) {
    return registerActor(vertx, entityAggregateClass, deploymentId)
      .flatMap(avoid -> vertx.deployVerticle(() -> new EntityActor<>(deploymentId, entityAggregateClass, ModuleBuilder.create().install(modules)), new DeploymentOptions()
          .setConfig(vertxConfiguration)
          .setInstances(CpuCoreSensor.availableProcessors() * 2)
        )
        .replaceWithVoid());
  }
}
