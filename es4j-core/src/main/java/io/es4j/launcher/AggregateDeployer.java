package io.es4j.launcher;

import io.es4j.Aggregate;


import io.es4j.Es4jDeployment;
import io.es4j.AsyncStateTransfer;
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
  private final Es4jDeployment es4jDeploymentConfiguration;
  private final Class<T> aggregateClass;
  private Infrastructure infrastructure;
  private List<AggregateServices> aggregateServices;
  private final Stack<String> deployed = new Stack<>();
  private CronTaskDeployer cronTaskDeployer;
  private TimerTaskDeployer timerTaskDeployer;

  public AggregateDeployer(
    final Class<T> aggregateClass,
    final Es4jDeployment es4jDeployment,
    final Vertx vertx,
    final String nodeDeploymentID
  ) {
    this.es4jDeploymentConfiguration = es4jDeployment;
    this.vertx = vertx;
    this.aggregateClass = aggregateClass;
    this.nodeDeploymentID = nodeDeploymentID;
  }

  public void deploy(final Promise<Void> startPromise) {
    Es4jConfigurationHandler.configure(
      vertx,
      es4jDeploymentConfiguration.infrastructureConfiguration(),
      infrastructureConfiguration -> {
        infrastructureConfiguration.put("schema", camelToKebab(es4jDeploymentConfiguration.aggregateClass().getSimpleName()));
        LOGGER.info("--- Es4j starting  {}::{} --- {}", es4jDeploymentConfiguration.infrastructureConfiguration(), this.nodeDeploymentID, infrastructureConfiguration.encodePrettily());
        close()
          .flatMap(avoid -> infrastructure(vertx, infrastructureConfiguration)
          )
          .call(injector -> {
              addHeartBeat();
              addProjections();
              final Supplier<Verticle> supplier = () -> new AggregateVerticle<>(es4jDeploymentConfiguration, aggregateClass, nodeDeploymentID);
              return startChannel(vertx, es4jDeploymentConfiguration.aggregateClass(), nodeDeploymentID)
                .flatMap(avoid -> Multi.createBy().repeating().uni(() ->vertx.deployVerticle(supplier, new DeploymentOptions()
                      .setConfig(infrastructureConfiguration)
                    )
                    .map(deployed::push)
                ).atMost(CpuCoreSensor.availableProcessors() * 2L).collect().asList()
                .call(__ -> {
                    this.aggregateServices = Es4jServiceLoader.loadAggregateServices();
                    return Es4jConfigurationHandler.fsConfigurations(vertx, es4jDeploymentConfiguration.fileBusinessRules())
                      .flatMap(av -> Multi.createFrom().iterable(aggregateServices)
                        .onItem().transformToUniAndMerge(
                          service -> service.start(es4jDeploymentConfiguration.aggregateClass(), vertx, infrastructureConfiguration)
                        )
                        .collect().asList()
                        .replaceWithVoid()
                      );
                  }
                )
                );
            }
          )
          .subscribe()
          .with(
            aVoid -> {
              startPromise.complete();
              LOGGER.info("--- Es4j  {} started ---", es4jDeploymentConfiguration.aggregateClass().getSimpleName());
            }
            , throwable -> {
              LOGGER.error("--- Es4j {} failed to start ---", es4jDeploymentConfiguration.aggregateClass().getSimpleName(), throwable);
              startPromise.fail(throwable);
            }
          );
      }
    );
  }

  private void addHeartBeat() {
    timerTaskDeployer.deploy(new AggregateHeartbeat<>(vertx, es4jDeploymentConfiguration.aggregateClass()));
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
    return infrastructure.setup(es4jDeploymentConfiguration, vertx, configuration)
      .invoke(avoid -> infrastructure.start(es4jDeploymentConfiguration, vertx, configuration));
  }


  private void addProjections() {
    final var aggregateProxy = new AggregateEventBusPoxy<>(vertx, aggregateClass);
    final var stateProjections = Es4jServiceLoader.stateProjections().stream()
      .filter(cc -> Es4jServiceLoader.getFirstGenericType(cc).isAssignableFrom(es4jDeploymentConfiguration.aggregateClass()))
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
      .filter(cc -> cc.aggregateClass().isAssignableFrom(es4jDeploymentConfiguration.aggregateClass()))
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

  private StateProjectionWrapper<T> gettStateProjectionWrapper(AsyncStateTransfer cc, Class<T> aggregateClass) {
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
    if (Objects.nonNull(aggregateServices) && !aggregateServices.isEmpty()) {
      closeUnis.addAll(aggregateServices.stream().map(AggregateServices::stop).toList());
    }
    if (!deployed.isEmpty()) {
      closeUnis.add(Multi.createBy().repeating().supplier(deployed::pop).whilst(__ -> !deployed.isEmpty())
        .onItem().transformToUniAndMerge(vertx::undeploy)
        .collect().asList()
        .replaceWithVoid()
      );
    }
    if (Objects.nonNull(infrastructure)) {
      closeUnis.add(infrastructure.stop());
    }
    if (!closeUnis.isEmpty()) {
      return Uni.join().all(closeUnis).andFailFast().replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }

}
