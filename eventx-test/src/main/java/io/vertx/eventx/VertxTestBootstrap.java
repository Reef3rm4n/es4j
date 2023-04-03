package io.vertx.eventx;

import io.activej.inject.module.Module;
import io.smallrye.mutiny.Multi;
import io.vertx.core.DeploymentOptions;
import io.vertx.eventx.config.ConfigurationEntry;
import io.vertx.eventx.launcher.EventxMain;
import io.vertx.eventx.sql.misc.Constants;
import io.vertx.eventx.sql.LiquibaseHandler;
import io.vertx.eventx.sql.RepositoryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.WebClient;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class VertxTestBootstrap {
  private static Network network = Network.newNetwork();
  public static final String POSTGRES_VERSION = "postgres:latest";
  private final Logger LOGGER = LoggerFactory.getLogger(VertxTestBootstrap.class);
  public PostgreSQLContainer<?> POSTGRES_CONTAINER;
  public static final Vertx VERTX = Vertx.vertx();
  public JsonObject CONFIGURATION = new JsonObject();
  public RepositoryHandler REPOSITORY_HANDLER;
  public WebClient WEB_CLIENT;
  public String configurationPath = System.getenv().getOrDefault("CONFIGURATION_FILE", "config.json");
  public Boolean postgres = Boolean.parseBoolean(System.getenv().getOrDefault("POSTGRES", "false"));
  public Boolean rabbitmq = Boolean.parseBoolean(System.getenv().getOrDefault("RABBITMQ", "false"));
  public Boolean kafka = Boolean.parseBoolean(System.getenv().getOrDefault("KAFKA", "false"));
  public Boolean REMOTE_TEST = Boolean.parseBoolean(System.getenv().getOrDefault("REMOTE_TEST", "false"));
  public String HTTP_HOST = System.getenv().getOrDefault("HTTP_HOST", "localhost");
  public Integer HTTP_PORT = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));
  public static final List<ConfigurationEntry> configurationEntries = new ArrayList<>();

  public static final List<String> collections = new ArrayList<>();

  private static final Map<String, Map<String, String>> liquibase = new HashMap<>();

  public VertxTestBootstrap addLiquibaseRun(String liquibaseChangelog, Map<String, String> params) {
    liquibase.put(liquibaseChangelog, params);
    return this;
  }

  public VertxTestBootstrap addModule(Module module) {
    EventxMain.MAIN_MODULES.add(module);
    return this;
  }

  public VertxTestBootstrap addCollection(final String collection) {
    collections.add(collection);
    return this;
  }

  public VertxTestBootstrap setRemoteHost(String host) {
    this.HTTP_HOST = host;
    return this;
  }

  public VertxTestBootstrap setRemotePort(Integer port) {
    this.HTTP_PORT = port;
    return this;
  }

  public VertxTestBootstrap addConfiguration(List<ConfigurationEntry> entries) {
    configurationEntries.addAll(entries);
    return this;
  }

  public VertxTestBootstrap setRemoteTest(final Boolean remoteTest) {
    this.REMOTE_TEST = remoteTest;
    return this;
  }

  public VertxTestBootstrap setConfigurationPath(final String configurationPath) {
    this.configurationPath = configurationPath;
    return this;
  }

  public VertxTestBootstrap setPostgres(final Boolean postgres) {
    this.postgres = postgres;
    return this;
  }

  public VertxTestBootstrap setRabbitmq(final Boolean rabbitmq) {
    this.rabbitmq = rabbitmq;
    return this;
  }

  public VertxTestBootstrap setKafka(final Boolean kafka) {
    this.kafka = kafka;
    return this;
  }

  public String configurationFile() {
    return configurationPath;
  }

  public Boolean postgresContainer() {
    return postgres;
  }

  public Boolean rabbitMQContainer() {
    return rabbitmq;
  }

  public Boolean kafkaContainer() {
    return kafka;
  }

  public Boolean remoteTest() {
    return REMOTE_TEST;
  }

  public void bootstrap() {
    CONFIGURATION = configuration();
    if (Boolean.TRUE.equals(postgresContainer())) {
      deployPgContainer();
    }
    REPOSITORY_HANDLER = RepositoryHandler.leasePool(CONFIGURATION, VERTX);
    if (!liquibase.isEmpty()) {
      Multi.createFrom().iterable(liquibase.entrySet())
        .onItem().transformToUniAndMerge(entry -> LiquibaseHandler.liquibaseString(REPOSITORY_HANDLER, entry.getKey(), entry.getValue()))
        .collect().asList()
        .await().indefinitely();
    }

    if (Boolean.TRUE.equals(remoteTest())) {
      WEB_CLIENT = WebClient.create(VERTX, new WebClientOptions()
          .setDefaultHost(HTTP_HOST)
          .setDefaultPort(HTTP_PORT)
//        .setUseAlpn(true)
//        .setTcpCork(true)
//        .setTcpFastOpen(true)
//        .setTcpKeepAlive(false)
//        .setTcpNoDelay(true)
//        .setTcpQuickAck(true)
      );
    } else {
      WEB_CLIENT = WebClient.create(VERTX, new WebClientOptions()
//        .setUseAlpn(true)
//        .setTcpCork(true)
//        .setTcpFastOpen(true)
//        .setTcpNoDelay(true)
//        .setTcpQuickAck(true)
      );
//      final var CFG = new Configuration<>(ConfigurationEntry.class, REPOSITORY_HANDLER);
//      if (!configurationEntries.isEmpty()) {
//        CFG.addAll(configurationEntries).onFailure(OrmConflictException.class)
//          .recoverWithUni(throwable -> CFG.updateAll(configurationEntries))
//          .await().indefinitely();
//      }
      VERTX.deployVerticle(EventxMain::new, new DeploymentOptions().setInstances(1).setConfig(CONFIGURATION)).await().indefinitely();

    }
  }

  public JsonObject configuration() {
    return VERTX.fileSystem().readFileBlocking(configurationFile()).toJsonObject();
  }

  public void deployPgContainer() {

    POSTGRES_CONTAINER = new PostgreSQLContainer<>(POSTGRES_VERSION)
      .withNetwork(network)
      .waitingFor(Wait.forListeningPort());
    POSTGRES_CONTAINER.start();
    CONFIGURATION.put(Constants.PG_HOST, POSTGRES_CONTAINER.getHost())
      .put(Constants.PG_PORT, POSTGRES_CONTAINER.getFirstMappedPort())
      .put(Constants.PG_USER, POSTGRES_CONTAINER.getUsername())
      .put(Constants.PG_PASSWORD, POSTGRES_CONTAINER.getPassword())
      .put(Constants.PG_DATABASE, POSTGRES_CONTAINER.getDatabaseName())
      .put(Constants.JDBC_URL, POSTGRES_CONTAINER.getJdbcUrl());
    VERTX.fileSystem().writeFileBlocking(configurationPath, Buffer.newInstance(CONFIGURATION.toBuffer()));
  }

  public void destroy() {
    VERTX.closeAndAwait();
    if (Boolean.TRUE.equals(postgresContainer())) {
      LOGGER.info(POSTGRES_CONTAINER.getLogs());
      POSTGRES_CONTAINER.stop();
    }
  }


}
