package io.es4j.launcher;

import io.es4j.*;
import io.es4j.core.tasks.AggregateHeartbeat;
import io.es4j.core.tasks.EventProjectionPoller;
import io.es4j.core.tasks.StateProjectionPoller;
import io.es4j.core.verticles.AggregateBridge;
import io.es4j.infrastructure.config.Es4jConfigurationHandler;
import io.es4j.infrastructure.misc.Es4jServiceLoader;
import io.es4j.task.CronTaskDeployer;
import io.es4j.task.TimerTaskDeployer;
import io.reactiverse.contextual.logging.ContextualData;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.vertx.UniHelper;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import org.crac.Context;
import org.crac.Core;
import org.crac.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.mutiny.core.eventbus.DeliveryContext;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Es4jMain extends AbstractVerticle implements Resource {

  protected static final Logger LOGGER = LoggerFactory.getLogger(Es4jMain.class);
  public static final List<Bootstrap> AGGREGATES = Es4jServiceLoader.bootstrapList();
  private CronTaskDeployer cronTaskDeployer;
  private TimerTaskDeployer timerTaskDeployer;
  public static final List<EventProjectionPoller> EVENT_PROJECTIONS = new ArrayList<>();
  public static final List<StateProjectionPoller> STATE_PROJECTIONS = new ArrayList<>();
  private static final List<AggregateDeployer<? extends Aggregate>> AGGREGATE_DEPLOYERS = new ArrayList<>();
  public static final Map<Class<? extends Aggregate>, List<Class<? extends Command>>> AGGREGATE_COMMANDS = new HashMap<>();
  public static final Map<Class<? extends Aggregate>, List<Class<Event>>> AGGREGATE_EVENTS = new HashMap<>();
  public static final List<AggregateHeartbeat<? extends Aggregate>> HEARTBEATS = new ArrayList<>();

  public Es4jMain() {
    Core.getGlobalContext().register(this);
  }

  @Override
  public void beforeCheckpoint(Context<? extends Resource> context) throws Exception {
    Promise<Void> p = Promise.promise();
    stop(p);
    CountDownLatch latch = new CountDownLatch(1);
    p.future().onComplete(event -> latch.countDown());
    latch.await();
  }

  @Override
  public void afterRestore(Context<? extends Resource> context) throws Exception {
    Promise<Void> p = Promise.promise();
    start(p);
    CountDownLatch latch = new CountDownLatch(1);
    p.future().onComplete(event -> latch.countDown());
    latch.await();
  }


  @Override
  public void start(final Promise<Void> startPromise) {
    LOGGER.info(" ---- Starting {}::{} ---- ", this.getClass().getName(), context.deploymentID());
    Infrastructure.setDroppedExceptionHandler(throwable -> LOGGER.error("[-- [Event.x]  had to drop the following exception --]", throwable));
    vertx.exceptionHandler(this::handleException);
    this.cronTaskDeployer = new CronTaskDeployer(vertx);
    this.timerTaskDeployer = new TimerTaskDeployer(vertx);
    addEventBusInterceptors();
    startAggregateResources(startPromise);
  }

  private void addEventBusInterceptors() {
    vertx.eventBus().addOutboundInterceptor(this::addContextualData);
    vertx.eventBus().addInboundInterceptor(this::addContextualData);
  }

  private void addContextualData(DeliveryContext<Object> event) {
    final var tenantID = event.message().headers().get("TENANT");
    final var aggregate = event.message().headers().get("AGGREGATE");
    if (tenantID != null) {
      ContextualData.put("TENANT", tenantID);
    }
    if (aggregate != null) {
      ContextualData.put("AGGREGATE", aggregate);
    }
    event.next();
  }

  private void startAggregateResources(final Promise<Void> startPromise) {
    AGGREGATES.stream()
      .map(aClass -> new AggregateDeployer<>(
          aClass.fileConfigurations(),
          aClass.aggregateClass(),
          vertx,
          context.deploymentID()
        )
      )
      .forEach(AGGREGATE_DEPLOYERS::add);
    final var aggregatesDeployment = AGGREGATE_DEPLOYERS.stream()
      .map(resource -> {
          final var promise = Promise.<Void>promise();
          resource.deploy(promise);
          return UniHelper.toUni(promise.future());
        }
      )
      .toList();
    if (aggregatesDeployment.isEmpty()) {
      throw new IllegalStateException("Aggregates not found");
    }
    Uni.join().all(aggregatesDeployment).andFailFast()
      .invoke(avoid -> deployHeartBeat())
      .invoke(avoid -> deployProjections())
      .flatMap(avoid -> deployBridges())
      .subscribe()
      .with(
        aVoid -> {
          startPromise.complete();
          LOGGER.info(" ----  {}::{} Started  ---- ", this.getClass().getName(), context.deploymentID());
        }
        , throwable -> {
          LOGGER.error(" ----  {}::{} Stopped  ---- ", this.getClass().getName(), context.deploymentID(), throwable);
          vertx.closeAndForget();
          startPromise.fail(throwable);
        }
      );
  }

  private void deployProjections() {
    EVENT_PROJECTIONS.forEach(cronTaskDeployer::deploy);
    STATE_PROJECTIONS.forEach(cronTaskDeployer::deploy);
  }

  private void deployHeartBeat() {
    HEARTBEATS.forEach(timerTaskDeployer::deploy);
  }

  private Uni<Void> deployBridges() {
    return vertx.deployVerticle(
        AggregateBridge::new,
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
    LOGGER.warn(" ---- Stopping  {}::{}  ---- ", this.getClass().getName(), context.deploymentID());
    close()
      .subscribe()
      .with(avoid -> stopPromise.complete(), stopPromise::fail);
  }


  private Uni<Void> close() {
    Es4jConfigurationHandler.close();
    timerTaskDeployer.close();
    cronTaskDeployer.close();
    return Multi.createFrom().iterable(AGGREGATE_DEPLOYERS)
      .onItem().transformToUniAndMerge(AggregateDeployer::close)
      .collect().asList()
      .replaceWithVoid();
  }
}
