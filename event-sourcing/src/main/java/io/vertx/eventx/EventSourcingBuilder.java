package io.vertx.eventx;

import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.actors.EntityActor;
import io.vertx.mutiny.core.Vertx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

import static io.vertx.eventx.actors.Channel.createChannel;


public class EventSourcingBuilder<T extends Aggregate> {
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

  public Uni<Void> deploy(String deploymendID) {
    final Supplier<Verticle> supplier = () -> new EntityActor<>( entityAggregateClass, ModuleBuilder.create().install(modules));
    return createChannel(vertx, entityAggregateClass, deploymendID)
      .flatMap(avoid -> vertx.deployVerticle(supplier, new DeploymentOptions()
            .setConfig(vertxConfiguration)
            .setInstances(CpuCoreSensor.availableProcessors() * 2)
          )
          .replaceWithVoid()
      );
  }
}
