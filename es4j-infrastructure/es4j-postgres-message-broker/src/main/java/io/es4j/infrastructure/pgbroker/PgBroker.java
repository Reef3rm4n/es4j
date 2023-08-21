package io.es4j.infrastructure.pgbroker;


import io.es4j.infrastructure.pgbroker.models.BrokerConfiguration;
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
import java.util.stream.IntStream;

public class PgBroker {
  private static final Logger LOGGER = LoggerFactory.getLogger(PgBroker.class);
  public static final Stack<String> deployments = new Stack<>();

  private PgBroker() {
  }

  public static Uni<Void> deploy(JsonObject configuration, Vertx vertx, Integer instances) {
    try {
      final var brokerConfiguration = BrokerConfiguration.defaultConfiguration();
      LOGGER.info("starting pg broker {}", brokerConfiguration);
      LOGGER.info("broker infra configuration {}", configuration.encodePrettily());
      final var repositoryHandler = RepositoryHandler.leasePool(configuration, vertx);
      return LiquibaseHandler.liquibaseString(repositoryHandler, "pg-broker.xml", Map.of("schema", repositoryHandler.configuration().getString("schema", EnvVars.SCHEMA)))
        .flatMap(avoid -> deploy(configuration, vertx, instances, brokerConfiguration));
    } catch (Exception e) {
      return Uni.createFrom().failure(e);
    }
  }

    private static Uni<Void> deploy(JsonObject configuration, Vertx vertx, Integer instances, BrokerConfiguration brokerConfiguration) {
        return Multi.createBy().repeating().uni(
                        () -> vertx.deployVerticle(new PgBrokerVerticle(brokerConfiguration), new DeploymentOptions().setConfig(configuration))
                                .map(deployments::push)
                ).atMost(instances).collect().asList()
                .replaceWithVoid();
    }

    public static Uni<Void> deploy(JsonObject configuration, Vertx vertx) {
    try {
      final var brokerConfiguration = BrokerConfiguration.defaultConfiguration();
      LOGGER.info("starting pg broker {}", brokerConfiguration);
      LOGGER.info("broker infra configuration {}", configuration.encodePrettily());
      final var repositoryHandler = RepositoryHandler.leasePool(configuration, vertx);
      return LiquibaseHandler.liquibaseString(repositoryHandler, "pg-broker.xml", Map.of("schema", repositoryHandler.configuration().getString("schema", EnvVars.SCHEMA)))
        .flatMap(avoid -> deploy(configuration,vertx,1,brokerConfiguration));
    } catch (Exception e) {
      return Uni.createFrom().failure(e);
    }
  }

  public static Uni<Void> deploy(JsonObject configuration, Vertx vertx, BrokerConfiguration brokerConfiguration) {
    try {
      LOGGER.info("starting pg broker {}", brokerConfiguration);
      LOGGER.info("broker infra configuration {}", configuration.encodePrettily());
      final var repositoryHandler = RepositoryHandler.leasePool(configuration, vertx);
      return LiquibaseHandler.liquibaseString(repositoryHandler, "pg-broker.xml", Map.of("schema", repositoryHandler.configuration().getString("schema", EnvVars.SCHEMA)))
        .flatMap(avoid -> deploy(configuration,vertx,CpuCoreSensor.availableProcessors(),brokerConfiguration)
        );
    } catch (Exception e) {
      return Uni.createFrom().failure(e);
    }
  }

  public static Uni<Void> undeploy(Vertx vertx) {
    if (!deployments.isEmpty()) {
      return Multi.createBy().repeating().supplier(deployments::pop)
              .whilst(__ -> !deployments.isEmpty())
        .onItem().transformToUniAndMerge(deploymentID -> {
          LOGGER.info("Dropping verticle -> {}", deploymentID);
          return vertx.undeploy(deploymentID)
                  .onFailure().invoke(throwable -> LOGGER.error("Unable to drop {}",deploymentID, throwable));
        })

        .collect().asList()
        .replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }


}
