package io.eventx.launcher;

import io.activej.inject.Injector;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.eventx.Aggregate;
import io.eventx.StateProjection;
import io.eventx.config.ConfigurationHandler;
import io.eventx.core.tasks.AggregateHeartbeat;
import io.eventx.core.verticles.AggregateVerticle;
import io.eventx.infrastructure.AggregateCache;
import io.eventx.infrastructure.EventStore;
import io.eventx.infrastructure.Infrastructure;
import io.eventx.infrastructure.OffsetStore;
import io.eventx.infrastructure.cache.CaffeineAggregateCache;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Verticle;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.eventx.core.projections.EventbusEventStream;
import io.eventx.core.tasks.EventProjectionPoller;
import io.eventx.core.tasks.StateProjectionPoller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.eventx.EventProjection;
import io.eventx.infrastructure.misc.CustomClassLoader;
import io.eventx.infrastructure.proxy.AggregateEventBusPoxy;
import io.eventx.core.objects.StateProjectionWrapper;
import io.vertx.mutiny.config.ConfigRetriever;
import io.vertx.mutiny.core.Vertx;

import java.util.ArrayList;
import java.util.function.Supplier;

import static io.eventx.core.CommandHandler.camelToKebab;
import static io.eventx.infrastructure.bus.AggregateBus.eventbusBridge;
import static io.eventx.launcher.EventxMain.*;

public class AggregateDeployer<T extends Aggregate> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AggregateDeployer.class);
  private final Vertx vertx;
  private final String deploymentID;
  private final ArrayList<Module> localModules;
  private ConfigRetriever deploymentConfiguration;
  private final Class<T> aggregateClass;
  private Infrastructure infrastructure;

  public AggregateDeployer(
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
        newConfiguration.put("schema", camelToKebab(aggregateClass.getSimpleName()));
        LOGGER.info("--- Event.x starting {}::{}--- {}", aggregateClass.getSimpleName(), this.deploymentID, newConfiguration.encodePrettily());
        close()
          .flatMap(avoid -> {
              moduleBuilder.bind(Vertx.class).toInstance(vertx);
              moduleBuilder.bind(JsonObject.class).toInstance(newConfiguration);
              return infrastructure(Injector.of(moduleBuilder.build()));
            }
          )
          .call(injector -> {
              addHeartBeat();
              addProjections(injector);
              final Supplier<Verticle> supplier = () -> new AggregateVerticle<>(aggregateClass, ModuleBuilder.create().install(localModules), deploymentID);
              return eventbusBridge(vertx, aggregateClass, deploymentID)
                .flatMap(avoid -> vertx.deployVerticle(supplier, new DeploymentOptions()
                      .setConfig(injector.getInstance(JsonObject.class))
                      .setInstances(CpuCoreSensor.availableProcessors() * 2)
                    )
                    .replaceWithVoid()
                )
                .call(avoid -> ConfigLauncher.addConfigurations(injector));
            }
          )
          .subscribe()
          .with(
            aVoid -> {
              startPromise.complete();
              LOGGER.info("--- Event.x {} started ---", aggregateClass.getSimpleName());
            }
            , throwable -> {
              LOGGER.error("--- Event.x {} failed to start ---", aggregateClass.getSimpleName(), throwable);
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
      new CaffeineAggregateCache(),
      injector.getInstance(EventStore.class),
      injector.getInstance(OffsetStore.class)
    );
    return infrastructure.start(aggregateClass, injector.getInstance(Vertx.class), injector.getInstance(JsonObject.class))
      .replaceWith(injector);
  }


  private void addProjections(Injector injector) {
    final var vertx = injector.getInstance(Vertx.class);
    final var aggregateProxy = new AggregateEventBusPoxy<>(vertx, aggregateClass);
    final var stateProjections = CustomClassLoader.loadFromInjector(injector, StateProjection.class).stream()
      .filter(cc -> CustomClassLoader.getFirstGenericType(cc).isAssignableFrom(aggregateClass))
      .map(cc -> new StateProjectionWrapper<T>(
        cc,
        aggregateClass,
        LoggerFactory.getLogger(cc.getClass())
      ))
      .map(tStateProjectionWrapper -> new StateProjectionPoller<T>(
        aggregateClass,
        tStateProjectionWrapper,
        aggregateProxy,
        infrastructure.eventStore(),
        infrastructure.offsetStore()
      ))
      .toList();
    final var eventProjections = CustomClassLoader.loadFromInjector(injector, EventProjection.class).stream()
      .filter(cc -> CustomClassLoader.getFirstGenericType(cc).isAssignableFrom(aggregateClass))
      .map(eventProjection -> new EventProjectionPoller(
          eventProjection,
          infrastructure.eventStore(),
          infrastructure.offsetStore()
        )
      )
      .toList();
    EVENT_PROJECTIONS.addAll(eventProjections);
    EVENT_PROJECTIONS.add(
      new EventProjectionPoller(
        new EventbusEventStream(vertx, aggregateClass),
        infrastructure.eventStore(),
        infrastructure.offsetStore()
      )
    );
    STATE_PROJECTIONS.addAll(stateProjections);
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
