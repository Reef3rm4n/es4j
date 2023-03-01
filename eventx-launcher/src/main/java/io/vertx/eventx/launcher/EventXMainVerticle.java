package io.vertx.eventx.launcher;

import io.activej.inject.module.Module;
import io.smallrye.mutiny.Multi;
import io.vertx.eventx.actors.ActorHeartbeat;
import io.vertx.eventx.config.ConfigurationHandler;
import io.vertx.eventx.config.ConfigurationDeployer;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.EventSourcingBuilder;
import io.vertx.eventx.common.CommandHeaders;
import io.vertx.eventx.queue.TaskProcessorVerticle;
import io.vertx.eventx.sql.LiquibaseHandler;
import io.vertx.eventx.sql.RepositoryHandler;
import io.activej.inject.Injector;
import io.activej.inject.module.ModuleBuilder;
import io.reactiverse.contextual.logging.ContextualData;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.config.ConfigRetriever;
import io.vertx.mutiny.core.Vertx;
import io.vertx.eventx.task.TimerTaskDeployer;
import io.vertx.eventx.common.CustomClassLoader;
import io.vertx.mutiny.core.eventbus.DeliveryContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class EventXMainVerticle extends AbstractVerticle {

  protected static final Logger LOGGER = LoggerFactory.getLogger(EventXMainVerticle.class);
  protected static final ConcurrentLinkedDeque<String> deploymentIds = new ConcurrentLinkedDeque<>();
  private RepositoryHandler repositoryHandler;
  private ConfigRetriever deploymentConfiguration;
  public static final String CONFIGURATION_NAME = System.getenv().getOrDefault("CONFIGURATION_NAME", "config");

  private final TimerTaskDeployer timerTaskDeployer = new TimerTaskDeployer();
  private final ConfigurationDeployer configurationDeployer = new ConfigurationDeployer();

  public static final Collection<Module> MODULES = new ArrayList<>();
  private List<ActorHeartbeat> heartbeat = new ArrayList<>();

  @Override
  public void start(final Promise<Void> startPromise) {
    LOGGER.info(" ---- Starting " + this.getClass().getSimpleName() + " " + this.deploymentID() + " ---- ");
    Infrastructure.setDroppedExceptionHandler(throwable -> LOGGER.error("[-- Mutiny [vert.x] Infrastructure had to drop the following exception --]", throwable));
    vertx.exceptionHandler(this::handleException);
    addEventBusInterceptors();
    bootstrap(startPromise);
  }

  private void addEventBusInterceptors() {
    vertx.eventBus().addOutboundInterceptor(EventXMainVerticle::addContextualData);

    vertx.eventBus().addInboundInterceptor(EventXMainVerticle::addContextualData);
  }

  private static void addContextualData(DeliveryContext<Object> event) {
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

  private void bootstrap(final Promise<Void> startPromise) {
    MODULES.addAll(CustomClassLoader.loadComponents());
    final var moduleBuilder = ModuleBuilder.create().install(MODULES);
    LOGGER.info("Bindings -> " + MODULES.stream().map(m -> m.getBindings().prettyPrint()).toList());
    this.deploymentConfiguration = ConfigurationHandler.configure(
      vertx,
      config().getString("configurationName", CONFIGURATION_NAME),
      newConfiguration -> {
        LOGGER.info("---------------------------------- Starting Vert.x -----------------------------------" + newConfiguration.encodePrettily());
        undeployComponent()
          .flatMap(avoid -> {
              this.repositoryHandler = RepositoryHandler.leasePool(newConfiguration, vertx);
              moduleBuilder.bind(RepositoryHandler.class).toInstance(repositoryHandler);
              moduleBuilder.bind(Vertx.class).toInstance(vertx);
              moduleBuilder.bind(JsonObject.class).toInstance(newConfiguration);
              final var injector = Injector.of(moduleBuilder.build());
              return LiquibaseHandler.handle(vertx, newConfiguration).replaceWith(injector);
            }
          )
//          .flatMap(injector -> configurationDeployer.deploy(injector, repositoryHandler).replaceWith(injector))
          .flatMap(injector -> TaskProcessorVerticle.deploy(vertx, newConfiguration, MODULES).replaceWith(injector))
          .flatMap(injector -> deployVerticles(newConfiguration, MODULES, injector).replaceWith(injector))
          .flatMap(injector -> deployEventSourcing(newConfiguration))
          .subscribe()
          .with(
            aVoid -> {
              startPromise.complete();
              LOGGER.info("---------------------------------- Vert.x MainVerticle Started -----------------------------------");
            }
            , throwable -> {
              LOGGER.error("---------------------- Error Deploying Vert.x MainVerticle ------------------------------------------", throwable);
              vertx.closeAndForget();
              startPromise.fail(throwable);
            }
          );
      }
    );
  }

  private Uni<Void> deployEventSourcing(JsonObject newConfiguration) {
    return Multi.createFrom().iterable(CustomClassLoader.getSubTypes(Aggregate.class))
      .onItem().transformToUniAndMerge(aggregateClass -> {
          heartbeat.add(new ActorHeartbeat<>(
              vertx,
              aggregateClass,
              1000L
            )
          );
          return new EventSourcingBuilder<>(aggregateClass)
            .setModules(MODULES)
            .setVertx(vertx)
            .setVertxConfiguration(newConfiguration)
            .deploy(context.deploymentID());
        }
      )
      .collect().asList()
      .replaceWithVoid();
  }


  private Uni<Void> deployVerticles(JsonObject newConfiguration, Collection<Module> modules, Injector injector) {
    if (CustomClassLoader.checkPresenceInModules(Verticle.class, modules)) {
      return Multi.createFrom().iterable(CustomClassLoader.loadFromInjector(injector, Verticle.class))
        .onItem().transformToUniAndMerge(
          verticle -> vertx.deployVerticle(verticle.supplier(), verticle.options().setConfig(newConfiguration))
        ).collect().last()
        .replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }


  private void handleException(Throwable throwable) {
    LOGGER.error("[-- MainVerticle had to drop the following exception --]", throwable);
  }

  @Override
  public void stop(final Promise<Void> stopPromise) {
    LOGGER.warn("Stopping " + this.getClass().getSimpleName() + " deploymentID " + context.deploymentID());
    undeployComponent().subscribe().with(avoid -> stopPromise.complete(), stopPromise::fail);
  }


  private Uni<Void> undeployComponent() {
    heartbeat.forEach(ActorHeartbeat::stop);
    if (deploymentConfiguration != null) {
      deploymentConfiguration.close();
    }
    timerTaskDeployer.stopTimers();
    if (configurationDeployer.listeners != null) {
      configurationDeployer.listeners.forEach(ConfigRetriever::close);
    }
    final var shutdowns = new ArrayList<Uni<Void>>();
    if (repositoryHandler != null) {
      shutdowns.add(repositoryHandler.shutDown());
    }
    if (configurationDeployer.pgSubscriber != null) {
      shutdowns.add(configurationDeployer.pgSubscriber.close());
    }
    if (!shutdowns.isEmpty()) {
      return Uni.join().all(shutdowns).andCollectFailures().replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }
}
