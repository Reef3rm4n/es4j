package io.es4j.launcher;

import io.es4j.Aggregate;


import io.es4j.Deployment;
import io.es4j.PollingStateProjection;
import io.es4j.core.tasks.AggregateHeartbeat;
import io.es4j.core.verticles.AggregateVerticle;
import io.es4j.infrastructure.*;
import io.es4j.infrastructure.cache.CaffeineAggregateCache;
import io.es4j.infrastructure.config.Es4jConfigurationHandler;
import io.es4j.infrastructure.misc.Es4jServiceLoader;
import io.es4j.task.CronTaskDeployer;
import io.es4j.task.TimerTaskDeployer;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Verticle;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.es4j.core.tasks.EventProjectionPoller;
import io.es4j.core.tasks.StateProjectionPoller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.es4j.infrastructure.proxy.AggregateEventBusPoxy;
import io.es4j.core.objects.StateProjectionWrapper;
import io.vertx.mutiny.core.Vertx;

import java.util.*;
import java.util.function.Supplier;

import static io.es4j.core.CommandHandler.camelToKebab;
import static io.es4j.infrastructure.bus.AggregateBus.startChannel;

public class AggregateDeployer<T extends Aggregate> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AggregateDeployer.class);
  private final Vertx vertx;
  private final String nodeDeploymentID;
  private final Deployment deploymentConfiguration;
  private final Class<T> aggregateClass;
  private Infrastructure infrastructure;
  private List<AggregateServices> aggregateServices;
  private final Set<String> deployed = new HashSet<>();
  private CronTaskDeployer cronTaskDeployer;
  private TimerTaskDeployer timerTaskDeployer;

  public AggregateDeployer(
    final Class<T> aggregateClass,
    final Deployment deployment,
    final Vertx vertx,
    final String nodeDeploymentID
  ) {
    this.deploymentConfiguration = deployment;
    this.vertx = vertx;
    this.aggregateClass = aggregateClass;
    this.nodeDeploymentID = nodeDeploymentID;
  }

  public void deploy(final Promise<Void> startPromise) {
    Es4jConfigurationHandler.configure(
      vertx,
      deploymentConfiguration.infrastructureConfiguration(),
      infrastructureConfiguration -> {
        infrastructureConfiguration.put("schema", camelToKebab(deploymentConfiguration.aggregateClass().getSimpleName()));
        LOGGER.info("--- Es4j starting  {}::{} --- {}", deploymentConfiguration.infrastructureConfiguration(), this.nodeDeploymentID, infrastructureConfiguration.encodePrettily());
        close()
          .flatMap(avoid -> infrastructure(vertx, infrastructureConfiguration)
          )
          .call(injector -> {
              addHeartBeat();
              addProjections();
              final Supplier<Verticle> supplier = () -> new AggregateVerticle<>(deploymentConfiguration, aggregateClass, nodeDeploymentID);
              return startChannel(vertx, deploymentConfiguration.aggregateClass(), nodeDeploymentID)
                .flatMap(avoid -> vertx.deployVerticle(supplier, new DeploymentOptions()
                      .setConfig(infrastructureConfiguration)
                      .setInstances(CpuCoreSensor.availableProcessors() * 2)
                    )
                    .map(deployed::add)
                )
                .call(avoid -> {
                    this.aggregateServices = Es4jServiceLoader.loadAggregateServices();
                    return Es4jConfigurationHandler.fsConfigurations(vertx, deploymentConfiguration.fileBusinessRules())
                      .flatMap(av -> Multi.createFrom().iterable(aggregateServices)
                        .onItem().transformToUniAndMerge(
                          service -> service.start(deploymentConfiguration.aggregateClass(), vertx, infrastructureConfiguration)
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
              LOGGER.info("--- Es4j  {} started ---", deploymentConfiguration.aggregateClass().getSimpleName());
            }
            , throwable -> {
              LOGGER.error("--- Es4j {} failed to start ---", deploymentConfiguration.aggregateClass().getSimpleName(), throwable);
              startPromise.fail(throwable);
            }
          );
      }
    );
  }

  private void addHeartBeat() {
    timerTaskDeployer.deploy(new AggregateHeartbeat<>(vertx, deploymentConfiguration.aggregateClass()));
  }

  private Uni<Void> infrastructure(Vertx vertx, JsonObject configuration) {
    this.infrastructure = new Infrastructure(
      Optional.of(new CaffeineAggregateCache()),
      Es4jServiceLoader.loadEventStore(),
      Optional.empty(),
      Es4jServiceLoader.loadOffsetStore()

    );
    if (Objects.isNull(cronTaskDeployer)) {
      cronTaskDeployer = new CronTaskDeployer(vertx);
    }
    if (Objects.isNull(timerTaskDeployer)) {
      timerTaskDeployer = new TimerTaskDeployer(vertx);
    }
    return infrastructure.setup(deploymentConfiguration, vertx, configuration);
  }


  private void addProjections() {
    final var aggregateProxy = new AggregateEventBusPoxy<>(vertx, aggregateClass);
    final var stateProjections = Es4jServiceLoader.stateProjections().stream()
      .filter(cc -> Es4jServiceLoader.getFirstGenericType(cc).isAssignableFrom(deploymentConfiguration.aggregateClass()))
      .map(cc -> gettStateProjectionWrapper(cc, aggregateClass))
      .map(tStateProjectionWrapper -> new StateProjectionPoller<T>(
        aggregateClass,
        tStateProjectionWrapper,
        aggregateProxy,
        infrastructure.eventStore(),
        infrastructure.offsetStore()
      ))
      .toList();
    final var eventProjections = Es4jServiceLoader.pollingEventProjections().stream()
      .filter(cc -> cc.aggregateClass().isAssignableFrom(deploymentConfiguration.aggregateClass()))
      .map(eventProjection -> new EventProjectionPoller(
          eventProjection,
          infrastructure.eventStore(),
          infrastructure.offsetStore()
        )
      )
      .toList();
    eventProjections.forEach(cronTaskDeployer::deploy);
    stateProjections.forEach(cronTaskDeployer::deploy);
  }

  private StateProjectionWrapper<T> gettStateProjectionWrapper(PollingStateProjection cc, Class<T> aggregateClass) {
    return new StateProjectionWrapper<T>(
      cc,
      aggregateClass,
      LoggerFactory.getLogger(cc.getClass())
    );
  }

  public Uni<Void> close() {
    final var closeUnis = new ArrayList<Uni<Void>>();
    if (Objects.nonNull(cronTaskDeployer)) {
      cronTaskDeployer.close();
    }
    if (Objects.nonNull(timerTaskDeployer)) {
      timerTaskDeployer.close();
    }
    if (!deployed.isEmpty()) {
      closeUnis.add(Multi.createFrom().iterable(deployed)
        .onItem().transformToUniAndMerge(deploymentID -> vertx.undeploy(deploymentID)
          .map(avoid -> deployed.remove(deploymentID)))
        .collect().asList()
        .replaceWithVoid()
      );
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
