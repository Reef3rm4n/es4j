package io.vertx.skeleton.framework;

import io.vertx.skeleton.ccp.QueueDeployer;
import io.vertx.skeleton.config.ConfigurationHandler;
import io.vertx.skeleton.config.ConfigurationDeployer;
import io.vertx.skeleton.models.RequestMetadata;
import io.vertx.skeleton.orm.LiquibaseHandler;
import io.vertx.skeleton.orm.RepositoryHandler;
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
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.skeleton.task.TaskDeployer;
import io.vertx.skeleton.utils.CustomClassLoader;
import org.reflections.Reflections;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedDeque;

public class SpineVerticle extends AbstractVerticle {

  protected static final Logger LOGGER = LoggerFactory.getLogger(SpineVerticle.class);
  protected static final ConcurrentLinkedDeque<String> deploymentIds = new ConcurrentLinkedDeque<>();
  private RepositoryHandler repositoryHandler;
  private ConfigRetriever deploymentConfiguration;
  public static final String PACKAGE_NAME = System.getenv().getOrDefault("PACKAGE_NAME", "io.vertx");
  public static final Reflections REFLECTIONS = new Reflections(PACKAGE_NAME);
  public static final String CONFIGURATION_NAME = System.getenv().getOrDefault("CONFIGURATION_NAME", "config");

  private final TaskDeployer taskDeployer = new TaskDeployer();
  private final ConfigurationDeployer configurationDeployer = new ConfigurationDeployer();

  @Override
  public void start(final Promise<Void> startPromise) {
    LOGGER.info(" ---- Starting " + this.getClass().getSimpleName() + " " + this.deploymentID() + " ---- ");
    Infrastructure.setDroppedExceptionHandler(throwable -> LOGGER.error("[-- Mutiny [vert.x] Infrastructure had to drop the following exception --]", throwable));
    vertx.exceptionHandler(this::handleException);
    addEventBusInterceptors();
    bootstrap(startPromise);
  }

  private void addEventBusInterceptors() {
    vertx.eventBus().addOutboundInterceptor(event -> {
        String requestId = ContextualData.get(RequestMetadata.X_TXT_ID);
        if (requestId != null) {
          event.message().headers().add(RequestMetadata.X_TXT_ID, requestId);
        }
        try {
          event.next();
        } catch (Exception e) {
          LOGGER.warn("Error in eventbus interceptor", e);
        }

      }
    );

    vertx.eventBus().addInboundInterceptor(event -> {
        String requestId = event.message().headers().get(RequestMetadata.X_TXT_ID);
        if (requestId != null) {
          ContextualData.put(RequestMetadata.X_TXT_ID, requestId);
        }
        event.next();
      }
    );
  }

  private void bootstrap(final Promise<Void> startPromise) {
    final var modules = CustomClassLoader.loadComponents();
    final var moduleBuilder = ModuleBuilder.create().install(CustomClassLoader.loadComponents());
    LOGGER.info("Bindings -> " + modules.stream().map(m -> m.getBindings().prettyPrint()).toList());
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
          .flatMap(injector -> configurationDeployer.deploy(injector, repositoryHandler).replaceWith(injector))
          .flatMap(injector -> RouterDeployer.deploy(repositoryHandler, deploymentIds, modules).replaceWith(injector))
          .flatMap(injector -> QueueDeployer.deploy(vertx, newConfiguration, modules).replaceWith(injector))
//          .flatMap(injector -> EventSourcingDeployer.deploy(vertx, repositoryHandler, deploymentIds, injector).replaceWith(injector))
          .invoke(injector -> taskDeployer.deploy(repositoryHandler, newConfiguration, injector))
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


  private void handleException(Throwable throwable) {
    LOGGER.error("[-- MainVerticle had to drop the following exception --]", throwable);
  }

  @Override
  public void stop(final Promise<Void> stopPromise) {
    LOGGER.warn("Stopping " + this.getClass().getSimpleName() + " deploymentID " + this.deploymentID());
    undeployComponent().subscribe().with(avoid -> stopPromise.complete(), stopPromise::fail);
  }

  private Uni<Void> undeployComponent() {
    if (deploymentConfiguration != null) {
      deploymentConfiguration.close();
    }
    taskDeployer.stopTimers();
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
