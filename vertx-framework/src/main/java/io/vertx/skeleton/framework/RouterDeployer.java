package io.vertx.skeleton.framework;


import io.vertx.skeleton.utils.CustomClassLoader;
import io.vertx.skeleton.httprouter.VertxHttpRoute;
import io.vertx.skeleton.httprouter.VertxHttpRouter;
import io.vertx.skeleton.orm.RepositoryHandler;
import io.activej.inject.module.Module;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import java.util.Collection;
import java.util.Deque;


public class RouterDeployer {
  protected static final Logger LOGGER = LoggerFactory.getLogger(RouterDeployer.class);

  private RouterDeployer() {
  }

  public static Uni<Void> deploy(RepositoryHandler repositoryHandler, final Deque<String> deploymentIds, final Collection<Module> modules) {
    if (CustomClassLoader.checkPresenceInModules(VertxHttpRoute.class, modules)) {
      LOGGER.info("Deploying http routes");
      return repositoryHandler.vertx().deployVerticle(
          () -> new VertxHttpRouter(modules),
          new DeploymentOptions()
            .setConfig(repositoryHandler.configuration())
            .setInstances(CpuCoreSensor.availableProcessors() * 2)
        )
        .map(deploymentIds::add)
        .replaceWithVoid();
    }
    LOGGER.info("No http routes to register");
    return Uni.createFrom().voidItem();
  }


}
