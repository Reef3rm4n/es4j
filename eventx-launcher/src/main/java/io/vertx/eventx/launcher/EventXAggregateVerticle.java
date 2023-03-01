package io.vertx.eventx.launcher;

import io.activej.inject.Injector;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.ModuleBuilder;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.actors.ChannelProxy;
import io.vertx.eventx.actors.EntityActor;
import io.vertx.eventx.actors.HttpProxy;
import io.vertx.eventx.actors.ProjectionUpdateActor;
import io.vertx.eventx.objects.ProjectionWrapper;
import io.vertx.eventx.storage.pg.mappers.EntityProjectionHistoryMapper;
import io.vertx.eventx.storage.pg.mappers.EventJournalMapper;
import io.vertx.eventx.storage.pg.mappers.EventJournalOffsetMapper;
import io.vertx.mutiny.core.Vertx;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Projection;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.eventx.common.CustomClassLoader;

import java.util.List;
import java.util.function.Supplier;

import static io.vertx.eventx.launcher.EventXMainVerticle.MODULES;

public class EventXAggregateVerticle<T extends Aggregate> implements Verticle {

  private final Class<T> aggregate;
  private final ModuleBuilder moduleBuilder;

  public EventXAggregateVerticle(
    Class<T> aggregate
  ) {
    addModules();
    this.aggregate = aggregate;
    this.moduleBuilder = ModuleBuilder.create().install(MODULES);
  }

  private void addModules() {
    addRoutesToModules();
    addProjectionsToModules();
  }

  private void addProjectionsToModules() {
    MODULES.add(
      new AbstractModule() {
        @Provides
        @Inject
        ProjectionUpdateActor<T> projectionUpdateActor(
          final List<ProjectionWrapper<T>> projections,
          final JsonObject configuration,
          final Vertx vertx
        ) {
          final var repositoryHandler = RepositoryHandler.leasePool(configuration, vertx, aggregate);
          return new ProjectionUpdateActor<>(
            null,
            new ChannelProxy<>(vertx, aggregate),
            new Repository<>(EventJournalMapper.INSTANCE, repositoryHandler),
            new Repository<>(EventJournalOffsetMapper.INSTANCE, repositoryHandler),
            new Repository<>(EntityProjectionHistoryMapper.INSTANCE, RepositoryHandler.leasePool(configuration, vertx, aggregate))
          );
        }

        @Provides
        @Inject
        List<ProjectionWrapper<T>> eventJournal(Injector injector) {
          return CustomClassLoader.loadFromInjector(injector, Projection.class).stream()
            .filter(projection -> CustomClassLoader.getFirstGenericType(projection).isAssignableFrom(aggregate))
            .map(projection -> new ProjectionWrapper<T>(
              projection,
              aggregate
            ))
            .toList();
        }

      }
    );
  }

  private void addRoutesToModules() {
    MODULES.add(
      new AbstractModule() {

        @Provides
        @Inject
        HttpProxy httpProxy(Vertx vertx) {
          return new HttpProxy(vertx, aggregate);
        }

      }
    );
  }


  @Override
  public DeploymentOptions options() {
    return new DeploymentOptions().setInstances(CpuCoreSensor.availableProcessors() * 2);
  }

  @Override
  public Supplier<io.vertx.core.Verticle> supplier() {
    return () -> new EntityActor<>(aggregate, moduleBuilder);
  }

}
