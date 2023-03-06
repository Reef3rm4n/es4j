package io.vertx.eventx.launcher;

import io.activej.inject.module.Module;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.vertx.UniHelper;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.common.CommandHeaders;
import io.vertx.eventx.http.HttpRouter;
import io.activej.inject.module.ModuleBuilder;
import io.reactiverse.contextual.logging.ContextualData;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.eventx.common.CustomClassLoader;
import io.vertx.mutiny.core.eventbus.DeliveryContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class EventxMain extends AbstractVerticle {

  protected static final Logger LOGGER = LoggerFactory.getLogger(EventxMain.class);
  public static final Collection<Module> MAIN_MODULES = new ArrayList<>(CustomClassLoader.loadComponents());
  private List<AggregateResources<? extends Aggregate>> resources = new ArrayList<>();

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
    final var commandID = event.message().headers().get(CommandHeaders.COMMAND_ID);
    final var tenantID = event.message().headers().get(CommandHeaders.TENANT_ID);
    if (commandID != null) {
      ContextualData.put(CommandHeaders.COMMAND_ID, commandID);
    }
    if (tenantID != null) {
      ContextualData.put(CommandHeaders.TENANT_ID, tenantID);
    }
    event.next();
  }

  private void startAggregateResources(final Promise<Void> startPromise) {
    CustomClassLoader.getSubTypes(Aggregate.class).stream()
      .map(aClass -> new AggregateResources<>(
          aClass,
          vertx,
          context.deploymentID()
        )
      )
      .forEach(resource -> resources.add(resource));
    final var futures = resources.stream()
      .map(resource -> {
          final var promise = Promise.<Void>promise();
          resource.deploy(promise);
          return UniHelper.toUni(promise.future());
        }
      )
      .toList();
    LOGGER.info("Bindings " + MAIN_MODULES.stream().map(m -> m.getBindings().prettyPrint()).toList());
    Uni.join().all(futures).andFailFast()
      .flatMap(avoid -> deployRoutes())
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

  private Uni<Void> deployRoutes() {
    // this deploys all the aggregate routes to all routers.
    final Supplier<Verticle> verticleSupplier = () -> new HttpRouter(ModuleBuilder.create().install(MAIN_MODULES));
    return vertx.deployVerticle(
        verticleSupplier, new DeploymentOptions()
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
    return Multi.createFrom().iterable(resources)
      .onItem().transformToUniAndMerge(AggregateResources::close)
      .collect().asList()
      .replaceWithVoid();
  }
}
