package io.vertx.eventx.launcher;

import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.reactiverse.contextual.logging.ContextualData;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.vertx.UniHelper;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.core.AggregateBridge;
import io.vertx.eventx.core.AggregateHeartbeat;
import io.vertx.eventx.core.EventProjectionPoller;
import io.vertx.eventx.core.StateProjectionPoller;
import io.vertx.eventx.objects.CommandHeaders;

import io.vertx.eventx.task.TimerTaskDeployer;
import io.vertx.mutiny.core.eventbus.DeliveryContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class EventxMain extends AbstractVerticle {

  protected static final Logger LOGGER = LoggerFactory.getLogger(EventxMain.class);
  public static final Collection<Module> MAIN_MODULES = new ArrayList<>(CustomClassLoader.loadModules());
  private static final List<AggregateResourcesAllocator<? extends Aggregate>> AGGREGATES = new ArrayList<>();
  public static final List<StateProjectionPoller<? extends Aggregate>> STATE_PROJECTIONS = new ArrayList<>();
  public static  final List<EventProjectionPoller> EVENT_PROJECTIONS = new ArrayList<>();
  public static final List<AggregateHeartbeat<? extends Aggregate>> HEARTBEATS = new ArrayList<>();
  private TimerTaskDeployer timers;

  @Override
  public void start(final Promise<Void> startPromise) {
    LOGGER.info(" ---- Starting " + this.getClass().getSimpleName() + " ----");
    Infrastructure.setDroppedExceptionHandler(throwable -> LOGGER.error("[-- [Event.x] Infrastructure had to drop the following exception --]", throwable));
    vertx.exceptionHandler(this::handleException);
    addEventBusInterceptors();
    startAggregateResources(startPromise);
  }

  private void addEventBusInterceptors() {
    vertx.eventBus().addOutboundInterceptor(this::addContextualData);
    vertx.eventBus().addInboundInterceptor(this::addContextualData);
  }

  private void addContextualData(DeliveryContext<Object> event) {
    final var commandID = event.message().headers().get("COMMAND");
    final var tenantID = event.message().headers().get("TENANT");
    final var aggregate = event.message().headers().get("AGGREGATE");
    if (commandID != null) {
      ContextualData.put("COMMAND", commandID);
    }
    if (tenantID != null) {
      ContextualData.put("TENANT", tenantID);
    }
    if (aggregate != null) {
      ContextualData.put("AGGREGATE", aggregate);
    }
    event.next();
  }

  private void startAggregateResources(final Promise<Void> startPromise) {
    LOGGER.info("Bindings " + MAIN_MODULES.stream().map(m -> m.getBindings().prettyPrint()).toList());
    CustomClassLoader.getSubTypes(Aggregate.class).stream()
      .map(aClass -> new AggregateResourcesAllocator<>(
          aClass,
          vertx,
          context.deploymentID()
        )
      )
      .forEach(AGGREGATES::add);
    final var aggregatesDeployment = AGGREGATES.stream()
      .map(resource -> {
          final var promise = Promise.<Void>promise();
          resource.deploy(promise);
          return UniHelper.toUni(promise.future());
        }
      )
      .toList();
    Uni.join().all(aggregatesDeployment).andFailFast()
      .flatMap(avoid -> deployBridges())
      .invoke(avoid -> deployProjections())
      .subscribe()
      .with(
        aVoid -> {
          startPromise.complete();
          LOGGER.info("---------------------------------- Event.x Main Started -----------------------------------");
        }
        , throwable -> {
          LOGGER.error("---------------------- Error starting Event.x ------------------------------------------", throwable);
          vertx.closeAndForget();
          startPromise.fail(throwable);
        }
      );
  }

  private void deployProjections() {
    this.timers = new TimerTaskDeployer(vertx);
    STATE_PROJECTIONS.forEach(this::deployProjection);
    EVENT_PROJECTIONS.forEach(this::deployProjection);
    HEARTBEATS.forEach(this::deployHeartBeat);
  }

  private void deployHeartBeat(AggregateHeartbeat<? extends Aggregate> aggregateHeartbeat) {
    timers.deploy(aggregateHeartbeat);
  }

  private void deployProjection(EventProjectionPoller eventProjectionPoller) {
    timers.deploy(eventProjectionPoller);
  }

  private void deployProjection(StateProjectionPoller<? extends Aggregate> stateProjectionPoller) {
    timers.deploy(stateProjectionPoller);
  }

  private Uni<Void> deployBridges() {
    return vertx.deployVerticle(
        () -> new AggregateBridge(ModuleBuilder.create().install(MAIN_MODULES)),
        new DeploymentOptions()
          .setInstances(CpuCoreSensor.availableProcessors() * 2)
      )
      .replaceWithVoid();
  }

  private void handleException(Throwable throwable) {
    LOGGER.error("[-- Event.x Main had to drop the following exception --]", throwable);
  }

  @Override
  public void stop(final Promise<Void> stopPromise) {
    LOGGER.warn("Stopping Event.x Main deploymentID " + context.deploymentID());
    undeployComponent()
      .subscribe()
      .with(avoid -> stopPromise.complete(), stopPromise::fail);
  }


  private Uni<Void> undeployComponent() {
    return Multi.createFrom().iterable(AGGREGATES)
      .onItem().transformToUniAndMerge(AggregateResourcesAllocator::close)
      .collect().asList()
      .replaceWithVoid();
  }
}
