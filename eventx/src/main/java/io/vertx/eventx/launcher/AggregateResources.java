package io.vertx.eventx.launcher;

import io.activej.inject.Injector;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Verticle;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.EventProjection;
import io.vertx.eventx.StateProjection;
import io.vertx.eventx.common.CustomClassLoader;
import io.vertx.eventx.config.ConfigurationHandler;
import io.vertx.eventx.core.AggregateHeartbeat;
import io.vertx.eventx.core.AggregateVerticle;
import io.vertx.eventx.core.EventProjectionPoller;
import io.vertx.eventx.core.StateProjectionPoller;
import io.vertx.eventx.infrastructure.*;
import io.vertx.eventx.infrastructure.proxies.AggregateEventBusClient;
import io.vertx.eventx.objects.StateProjectionWrapper;
import io.vertx.eventx.task.TimerTaskDeployer;
import io.vertx.mutiny.config.ConfigRetriever;
import io.vertx.mutiny.core.Vertx;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static io.vertx.eventx.infrastructure.bus.AggregateBus.createChannel;
import static io.vertx.eventx.launcher.EventxMain.MAIN_MODULES;

public class AggregateResources<T extends Aggregate> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AggregateResources.class);
  private final Vertx vertx;
  private final String deploymentID;
  private final ArrayList<Module> localModules;
  private ConfigRetriever deploymentConfiguration;
  private final Class<T> aggregateClass;
  private Infrastructure infrastructure;

  public AggregateResources(
    Class<T> aggregateClass,
    Vertx vertx,
    String deploymentID
  ) {
    this.aggregateClass = aggregateClass;
    this.vertx = vertx;
    this.deploymentID = deploymentID;
    this.localModules = new ArrayList<>(MAIN_MODULES);
  }


  public void deploy(final Promise<Void> startPromise) {
    injectBusHeartBeat();
    injectProjectionPollers();
    injectProjectionPollers();
    final var moduleBuilder = ModuleBuilder.create().install(localModules);
    this.deploymentConfiguration = ConfigurationHandler.configure(
      vertx,
      aggregateClass.getSimpleName().toLowerCase(),
      newConfiguration -> {
        LOGGER.info("---------------------------------- Starting Event.x Aggregate " + aggregateClass.getSimpleName() + "-----------------------------------" + newConfiguration.encodePrettily());
        close()
          .flatMap(avoid -> {
              moduleBuilder.bind(Vertx.class).toInstance(vertx);
              moduleBuilder.bind(JsonObject.class).toInstance(newConfiguration);
              return infrastructure(Injector.of(moduleBuilder.build()));
            }
          )
          .call(injector -> {
              final Supplier<Verticle> supplier = () -> new AggregateVerticle<>(aggregateClass, ModuleBuilder.create().install(localModules));
              return createChannel(vertx, aggregateClass, deploymentID)
                .flatMap(avoid -> vertx.deployVerticle(supplier, new DeploymentOptions()
                      .setConfig(newConfiguration)
                      .setInstances(CpuCoreSensor.availableProcessors() * 2)
                    )
                    .replaceWithVoid()
                );
            }
          )
          .subscribe()
          .with(
            aVoid -> {
              startPromise.complete();
              LOGGER.info("---------------------------------- Event.x started aggregate " + aggregateClass.getSimpleName() + " -----------------------------------");
            }
            , throwable -> {
              LOGGER.error("---------------------- Error deploying Event.x aggregate " + aggregateClass.getSimpleName() + " ---------------------------------------", throwable);
              startPromise.fail(throwable);
            }
          );
      }
    );
  }

  private Uni<Injector> infrastructure(Injector injector) {
    this.infrastructure = new Infrastructure(
      injector.getInstance(AggregateCache.class),
      injector.getInstance(EventStore.class),
      injector.getInstance(OffsetStore.class)
    );
    return infrastructure.start().replaceWith(injector);
  }


  private void injectProjectionPollers() {
    MAIN_MODULES.add(
      new AbstractModule() {
        @Provides
        @Inject
        StateProjectionPoller<T> stateProjectionPoller(
          final List<StateProjectionWrapper<T>> projections,
          final EventStore eventStore,
          final OffsetStore offsetStore
        ) {
          return new StateProjectionPoller<>(
            projections,
            new AggregateEventBusClient<>(vertx, aggregateClass),
            eventStore,
            offsetStore
          );
        }

        @Provides
        @Inject
        List<StateProjectionWrapper<T>> stateProjectionWrappers(Injector injector) {
          return CustomClassLoader.loadFromInjector(injector, StateProjection.class).stream()
            .filter(stateProjection -> CustomClassLoader.getFirstGenericType(stateProjection).isAssignableFrom(aggregateClass))
            .map(stateProjection -> new StateProjectionWrapper<T>(
              stateProjection,
              aggregateClass
            ))
            .toList();
        }
      }
    );
  }

  private void injectEventPollers() {
    MAIN_MODULES.add(
      new AbstractModule() {
        @Provides
        @Inject
        EventProjectionPoller eventPoller(
          final List<EventProjection> eventProjections,
          final EventStore eventStore,
          final OffsetStore offsetStore
        ) {
          return new EventProjectionPoller(eventProjections,eventStore,offsetStore);
        }

        @Provides
        @Inject
        List<EventProjection> stateProjectionWrappers(Injector injector) {
          return CustomClassLoader.loadFromInjector(injector, EventProjection.class).stream()
            .filter(stateProjection -> CustomClassLoader.getFirstGenericType(stateProjection).isAssignableFrom(aggregateClass))
            .toList();
        }
      }
    );
  }

  private void injectBusHeartBeat() {
    localModules.add(
      new AbstractModule() {
        @Inject
        @Provides
        AggregateHeartbeat<T> heartbeat(Vertx vertx) {
          return new AggregateHeartbeat<>(vertx, aggregateClass);
        }
      }
    );
  }

  public Uni<Void> close() {
    deploymentConfiguration.close();
    return infrastructure.stop();
  }

}
