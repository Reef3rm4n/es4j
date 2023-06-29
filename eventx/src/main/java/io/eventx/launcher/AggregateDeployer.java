package io.eventx.launcher;

import io.eventx.Aggregate;


import io.eventx.core.tasks.AggregateHeartbeat;
import io.eventx.core.verticles.AggregateVerticle;
import io.eventx.infrastructure.*;
import io.eventx.infrastructure.cache.CaffeineAggregateCache;
import io.eventx.infrastructure.config.EventxConfigurationHandler;
import io.eventx.infrastructure.misc.EventxClassLoader;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Verticle;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.eventx.core.tasks.EventProjectionPoller;
import io.eventx.core.tasks.StateProjectionPoller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.eventx.infrastructure.proxy.AggregateEventBusPoxy;
import io.eventx.core.objects.StateProjectionWrapper;
import io.vertx.mutiny.core.Vertx;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.eventx.core.CommandHandler.camelToKebab;
import static io.eventx.infrastructure.bus.AggregateBus.startChannel;
import static io.eventx.launcher.EventxMain.*;

public class AggregateDeployer<T extends Aggregate> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AggregateDeployer.class);
  private final Vertx vertx;
  private final String deploymentID;
  private final Class<T> aggregateClass;
  private final List<String> files;
  private Infrastructure infrastructure;
  private List<AggregateServices> aggregateServices;

  public AggregateDeployer(
    final List<String> files,
    final Class<T> aggregateClass,
    final Vertx vertx,
    final String deploymentID
  ) {
    this.files = files;
    this.aggregateClass = aggregateClass;
    this.vertx = vertx;
    this.deploymentID = deploymentID;
  }

  public void deploy(final Promise<Void> startPromise) {
    EventxConfigurationHandler.configure(
      vertx,
      camelToKebab(aggregateClass.getSimpleName()),
      newConfiguration -> {
        newConfiguration.put("schema", camelToKebab(aggregateClass.getSimpleName()));
        LOGGER.info("--- Event.x starting {}::{} --- {}", aggregateClass.getSimpleName(), this.deploymentID, newConfiguration.encodePrettily());
        close()
          .flatMap(avoid -> infrastructure(vertx, newConfiguration)
          )
          .call(injector -> {
              addHeartBeat();
              addProjections();
              final Supplier<Verticle> supplier = () -> new AggregateVerticle<>(aggregateClass, deploymentID);
              return startChannel(vertx, aggregateClass, deploymentID)
                .flatMap(avoid -> vertx.deployVerticle(supplier, new DeploymentOptions()
                      .setConfig(newConfiguration)
                      .setInstances(CpuCoreSensor.availableProcessors() * 2)
                    )
                    .replaceWithVoid()
                )
                .call(avoid -> {
                    this.aggregateServices = EventxClassLoader.loadAggregateServices();
                    return EventxConfigurationHandler.fsConfigurations(vertx, files)
                      .flatMap(av -> Multi.createFrom().iterable(aggregateServices)
                        .onItem().transformToUniAndMerge(
                          service -> service.start(aggregateClass, vertx, newConfiguration)
                        )
                        .collect().asList()
                        .replaceWithVoid()
                      );
                  }
                );
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

  private Uni<Void> infrastructure(Vertx vertx, JsonObject configuration) {
    this.infrastructure = new Infrastructure(
      Optional.of(new CaffeineAggregateCache()),
      EventxClassLoader.loadEventStore(),
      Optional.empty(),
      EventxClassLoader.loadOffsetStore()

    );
    return infrastructure.setup(aggregateClass, vertx, configuration);
  }


  private void addProjections() {
    final var aggregateProxy = new AggregateEventBusPoxy<>(vertx, aggregateClass);
    final var stateProjections = EventxClassLoader.stateProjections().stream()
      .filter(cc -> EventxClassLoader.getFirstGenericType(cc).isAssignableFrom(aggregateClass))
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
    final var eventProjections = EventxClassLoader.pollingEventProjections().stream()
      .filter(cc -> cc.aggregateClass().isAssignableFrom(aggregateClass))
      .map(eventProjection -> new EventProjectionPoller(
          eventProjection,
          infrastructure.eventStore(),
          infrastructure.offsetStore()
        )
      )
      .toList();
    EVENT_PROJECTIONS.addAll(eventProjections);
    STATE_PROJECTIONS.addAll(stateProjections);
  }

  public Uni<Void> close() {
    final var closeUnis = new ArrayList<Uni<Void>>();
    if (infrastructure != null) {
      closeUnis.add(infrastructure.stop());
    }
    if (!closeUnis.isEmpty()) {
      return Uni.join().all(closeUnis).andFailFast().replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }

}
