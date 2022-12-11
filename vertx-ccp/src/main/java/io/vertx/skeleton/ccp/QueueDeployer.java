package io.vertx.skeleton.ccp;

import io.vertx.skeleton.utils.CustomClassLoader;
import io.activej.inject.module.Module;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


public class QueueDeployer {

  protected static final Logger LOGGER = LoggerFactory.getLogger(QueueDeployer.class);
  public static final AtomicBoolean QUEUES_CHECKED = new AtomicBoolean(false);

  public static Uni<Void> deploy(
    final Vertx vertx,
    final JsonObject newConfiguration,
    final Collection<Module> modules
  ) {
    if (CustomClassLoader.checkPresenceInModules(SingleProcessConsumer.class, modules)) {
      LOGGER.info("Deploying queue consumers ...");
      return vertx.deployVerticle(
        () -> new QueueConsumerVerticle(modules),
        new DeploymentOptions()
          .setInstances(CpuCoreSensor.availableProcessors() * 2)
          .setConfig(newConfiguration)
      ).replaceWithVoid();
    }
    LOGGER.info("Concurrent queues not present !");
    return Uni.createFrom().voidItem();

  }

}
