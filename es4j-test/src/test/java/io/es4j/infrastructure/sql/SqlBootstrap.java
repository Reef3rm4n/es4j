package io.es4j.infrastructure.sql;

import io.es4j.Aggregate;
import io.es4j.sql.LiquibaseHandler;
import io.es4j.sql.RepositoryHandler;
import io.es4j.sql.misc.Constants;
import io.smallrye.mutiny.Multi;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;


public class SqlBootstrap {
  private static Network network = Network.newNetwork();
  public static final String POSTGRES_VERSION = "postgres:latest";
  private final Logger LOGGER = LoggerFactory.getLogger(SqlBootstrap.class);
  public PostgreSQLContainer<?> POSTGRES_CONTAINER;
  public GenericContainer redis = new GenericContainer(DockerImageName.parse("redis:latest"))
    .withExposedPorts(6379);
  public static Vertx vertx;
  public JsonObject CONFIGURATION = new JsonObject();
  public static RepositoryHandler REPOSITORY_HANDLER;
  public WebClient WEB_CLIENT;
  public String configurationPath = System.getenv().getOrDefault("CONFIGURATION_FILE", "config.json");
  public Boolean postgres = false;
  public String HTTP_HOST = System.getenv().getOrDefault("HTTP_HOST", "localhost");
  public Integer HTTP_PORT = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));

  public String schema = System.getenv().getOrDefault("SCHEMA", "es4j");

  private static final Map<String, Map<String, String>> liquibase = new HashMap<>();

  public void start() {
    vertx = Vertx.vertx();
    CONFIGURATION = configuration().put("schema", schema);
    if (postgres) {
      deployPgContainer();
    }
    REPOSITORY_HANDLER = RepositoryHandler.leasePool(CONFIGURATION, vertx);
    if (!liquibase.isEmpty()) {
      Multi.createFrom().iterable(liquibase.entrySet())
        .onItem().transformToUniAndMerge(entry -> LiquibaseHandler.liquibaseString(REPOSITORY_HANDLER, entry.getKey(), entry.getValue()))
        .collect().asList()
        .await().indefinitely();
    }
    WEB_CLIENT = WebClient.create(vertx, new WebClientOptions()
      .setDefaultHost(HTTP_HOST)
      .setDefaultPort(HTTP_PORT)
    );
  }


  public SqlBootstrap addLiquibaseRun(String liquibaseChangelog, Map<String, String> params) {
    liquibase.put(liquibaseChangelog, params);
    return this;
  }

  public SqlBootstrap setRemoteHost(String host) {
    this.HTTP_HOST = host;
    return this;
  }

  public SqlBootstrap setRemotePort(Integer port) {
    this.HTTP_PORT = port;
    return this;
  }

  public SqlBootstrap setConfigurationPath(final String configurationPath) {
    this.configurationPath = configurationPath;
    return this;
  }

  public SqlBootstrap setPostgres(final Boolean postgres) {
    this.postgres = postgres;
    return this;
  }

  public String configurationFile() {
    return configurationPath;
  }

  public Boolean postgresContainer() {
    return postgres;
  }

  public String schema() {
    return schema == null ? "postgres" : schema;
  }

  public SqlBootstrap schema(String schema) {
    this.schema = schema;
    return this;
  }

  public JsonObject configuration() {
    return vertx.fileSystem().readFileBlocking(configurationFile()).toJsonObject();
  }

  public JsonObject configuration(Class<? extends Aggregate> aggregateClass) {
    return vertx.fileSystem().readFileBlocking(aggregateClass.getSimpleName() + ".json").toJsonObject();
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
    vertx.fileSystem().writeFileBlocking(configurationPath, Buffer.newInstance(CONFIGURATION.toBuffer()));
  }

  public void destroy() {
    vertx.closeAndAwait();
    if (Boolean.TRUE.equals(postgresContainer())) {
      LOGGER.info(POSTGRES_CONTAINER.getLogs());
      POSTGRES_CONTAINER.stop();
      POSTGRES_CONTAINER.close();
    }
  }


}
