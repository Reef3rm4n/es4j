package io.vertx.eventx.launcher;

import io.activej.inject.Injector;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Verticle;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.EventProjection;
import io.vertx.eventx.StateProjection;
import io.vertx.eventx.config.ConfigurationHandler;
import io.vertx.eventx.core.*;
import io.vertx.eventx.infrastructure.*;
import io.vertx.eventx.infrastructure.misc.CustomClassLoader;
import io.vertx.eventx.infrastructure.proxy.AggregateEventBusPoxy;
import io.vertx.eventx.objects.StateProjectionWrapper;
import io.vertx.mutiny.config.ConfigRetriever;
import io.vertx.mutiny.core.Vertx;

import java.util.ArrayList;
import java.util.function.Supplier;

import static io.vertx.eventx.infrastructure.bus.AggregateBus.eventbusBridge;
import static io.vertx.eventx.launcher.EventxMain.*;

public class AggregateLauncher<T extends Aggregate> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AggregateLauncher.class);
  private final Vertx vertx;
  private final String deploymentID;
  private final ArrayList<Module> localModules;
  private ConfigRetriever deploymentConfiguration;
  private final Class<T> aggregateClass;
  private Infrastructure infrastructure;

  public AggregateLauncher(
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
    final var moduleBuilder = ModuleBuilder.create().install(localModules);
    this.deploymentConfiguration = ConfigurationHandler.configure(
      vertx,
      aggregateClass.getSimpleName().toLowerCase(),
      newConfiguration -> {
        LOGGER.info(" --- Event.x starting aggregate {} ---", aggregateClass.getSimpleName());
        close()
          .flatMap(avoid -> {
              moduleBuilder.bind(Vertx.class).toInstance(vertx);
              moduleBuilder.bind(JsonObject.class).toInstance(newConfiguration);
              return infrastructure(Injector.of(moduleBuilder.build()));
            }
          )
          .call(injector -> {
              addPersistentProjections(injector);
              addHeartBeat();
              final Supplier<Verticle> supplier = () -> new AggregateVerticle<>(aggregateClass, ModuleBuilder.create().install(localModules), deploymentID);
              return eventbusBridge(vertx, aggregateClass, deploymentID)
                .flatMap(avoid -> vertx.deployVerticle(supplier, new DeploymentOptions()
                      .setConfig(newConfiguration)
                      .setInstances(CpuCoreSensor.availableProcessors() * 2)
                    )
                    .replaceWithVoid()
                )
                .call(avoid -> ConfigLauncher.addConfigurations(injector))
                .invoke(avoid -> CronTaskLauncher.addProjections(injector));
            }
          )
          .subscribe()
          .with(
            aVoid -> {
              startPromise.complete();
              LOGGER.info(" --- Event.x aggregate {} started ---", aggregateClass.getSimpleName());
            }
            , throwable -> {
              LOGGER.error(" --- Error starting aggregate {} --- ", aggregateClass.getSimpleName(), throwable);
              startPromise.fail(throwable);
            }
          );
      }
    );
  }

  private void addHeartBeat() {
    HEARTBEATS.add(new AggregateHeartbeat<>(vertx, aggregateClass));
  }

  private Uni<Injector> infrastructure(Injector injector) {
    this.infrastructure = new Infrastructure(
      injector.getInstance(AggregateCache.class),
      injector.getInstance(EventStore.class),
      injector.getInstance(OffsetStore.class)
    );
    return infrastructure.start().replaceWith(injector);
  }


  private void addPersistentProjections(Injector injector) {
    STATE_PROJECTIONS.add(new StateProjectionPoller<>(
        aggregateClass,
        CustomClassLoader.loadFromInjector(injector, StateProjection.class).stream()
          .filter(stateProjection -> CustomClassLoader.getFirstGenericType(stateProjection).isAssignableFrom(aggregateClass))
          .map(stateProjection -> new StateProjectionWrapper<T>(
            stateProjection,
            aggregateClass
          ))
          .toList(),
        new AggregateEventBusPoxy<>(vertx, aggregateClass),
        infrastructure.eventStore(),
        infrastructure.offsetStore()
      )
    );
    EVENT_PROJECTIONS.add(
      new EventProjectionPoller(CustomClassLoader.loadFromInjector(injector, EventProjection.class).stream()
        .filter(stateProjection -> CustomClassLoader.getFirstGenericType(stateProjection).isAssignableFrom(aggregateClass))
        .toList(),
        infrastructure.eventStore(),
        infrastructure.offsetStore())
    );
  }

  public Uni<Void> close() {
    final var closeUnis = new ArrayList<Uni<Void>>();
    if (!ConfigLauncher.CONFIG_RETRIEVERS.isEmpty()) {
      closeUnis.add(ConfigLauncher.close());
    }
    if (deploymentConfiguration != null) {
      deploymentConfiguration.close();
    }
    if (infrastructure != null) {
      closeUnis.add(infrastructure.stop());
    }
    if (!closeUnis.isEmpty()) {
      return Uni.join().all(closeUnis).andFailFast().replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }

}
