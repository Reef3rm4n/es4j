package io.es4j.infrastructure.pgbroker;


import io.es4j.infrastructure.pgbroker.models.PgBrokerConfiguration;
import io.es4j.sql.LiquibaseHandler;
import io.es4j.sql.RepositoryHandler;
import io.es4j.sql.misc.EnvVars;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;

public class PgBroker {
  private static final Logger LOGGER = LoggerFactory.getLogger(PgBroker.class);
  private static final List<String> deployments = new ArrayList<>();

  private PgBroker() {
  }

  public static Uni<Void> deploy(JsonObject configuration, Vertx vertx, Integer instances) {
    try {
      final var brokerConfiguration = PgBrokerConfiguration.defaultConfiguration();
      LOGGER.info("starting pg broker {}", brokerConfiguration);
      LOGGER.info("broker infra configuration {}", configuration.encodePrettily());
      final var repositoryHandler = RepositoryHandler.leasePool(configuration, vertx);
      return LiquibaseHandler.liquibaseString(repositoryHandler, "queue.xml", Map.of("schema", repositoryHandler.configuration().getString("schema", EnvVars.SCHEMA)))
        .flatMap(avoid -> {
            Supplier<Verticle> supplier = () -> new PgBrokerVerticle(brokerConfiguration);
            return repositoryHandler.vertx().deployVerticle(supplier, new DeploymentOptions().setInstances(instances).setConfig(configuration))
              .map(deployments::add)
              .replaceWithVoid();
          }
        );
    } catch (Exception e) {
      return Uni.createFrom().failure(e);
    }
  }
  public static Uni<Void> deploy(JsonObject configuration, Vertx vertx) {
    try {
      final var brokerConfiguration = PgBrokerConfiguration.defaultConfiguration();
      LOGGER.info("starting pg broker {}", brokerConfiguration);
      LOGGER.info("broker infra configuration {}", configuration.encodePrettily());
      final var repositoryHandler = RepositoryHandler.leasePool(configuration, vertx);
      return LiquibaseHandler.liquibaseString(repositoryHandler, "queue.xml", Map.of("schema", repositoryHandler.configuration().getString("schema", EnvVars.SCHEMA)))
        .flatMap(avoid -> {
            Supplier<Verticle> supplier = () -> new PgBrokerVerticle(brokerConfiguration);
            return repositoryHandler.vertx().deployVerticle(supplier, new DeploymentOptions().setInstances(1).setConfig(configuration))
              .map(deployments::add)
              .replaceWithVoid();
          }
        );
    } catch (Exception e) {
      return Uni.createFrom().failure(e);
    }
  }

  public static Uni<Void> deploy(JsonObject configuration, Vertx vertx, PgBrokerConfiguration brokerConfiguration) {
    try {
      LOGGER.info("starting pg broker {}", brokerConfiguration);
      LOGGER.info("broker infra configuration {}", configuration.encodePrettily());
      final var repositoryHandler = RepositoryHandler.leasePool(configuration, vertx);
      return LiquibaseHandler.liquibaseString(repositoryHandler, "queue.xml", Map.of("schema", repositoryHandler.configuration().getString("schema", EnvVars.SCHEMA)))
        .flatMap(avoid -> {
            Supplier<Verticle> supplier = () -> new PgBrokerVerticle(brokerConfiguration);
            return repositoryHandler.vertx().deployVerticle(supplier, new DeploymentOptions().setInstances(CpuCoreSensor.availableProcessors()).setConfig(configuration))
              .map(deployments::add)
              .replaceWithVoid();
          }
        );
    } catch (Exception e) {
      return Uni.createFrom().failure(e);
    }
  }

  public static Uni<Void> undeploy(Vertx vertx) {
    if (!deployments.isEmpty()) {
      return Multi.createFrom().iterable(deployments)
        .onItem().transformToUniAndMerge(vertx::undeploy)
        .collect().asList()
        .replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }


}
