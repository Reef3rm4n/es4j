package io.eventx;

import io.activej.inject.Injector;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.eventx.infra.pg.PgEventStore;
import io.eventx.infrastructure.misc.CustomClassLoader;
import io.eventx.sql.misc.Constants;
import io.vertx.core.DeploymentOptions;
import io.eventx.infrastructure.proxy.AggregateEventBusPoxy;
import io.eventx.launcher.EventxMain;
import io.eventx.sql.RepositoryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.WebClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import static io.eventx.core.CommandHandler.camelToKebab;


public class Bootstrapper<T extends Aggregate> {
  public AggregateEventBusPoxy<T> eventBusPoxy;
  public AggregateHttpClient<T> httpClient;
  private static Network network = Network.newNetwork();
  public static final String POSTGRES_VERSION = "postgres:latest";
  public static final String ZOOKEEPER_VERSION = "bitnami/zookeeper:latest";
  private final Logger LOGGER = LoggerFactory.getLogger(Bootstrapper.class);
  public PostgreSQLContainer<?> postgreSQLContainer;
  public GenericContainer redis;

  public static Vertx vertx;

  public JsonObject config;
  public static RepositoryHandler repositoryHandler;
  public static Injector injector;
  public Boolean postgres = Boolean.parseBoolean(System.getenv().getOrDefault("POSTGRES", "false"));
  public Boolean clustered = Boolean.parseBoolean(System.getenv().getOrDefault("CLUSTERED", "false"));
  public String HTTP_HOST = System.getenv().getOrDefault("HTTP_HOST", "localhost");
  public Integer HTTP_PORT = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));
  public Class<T> aggregateClass;
  private GenericContainer zookeeperContainer;

  public Bootstrapper(
    Class<T> aggregateClass
  ) {
    vertx = Vertx.vertx();
    this.aggregateClass = aggregateClass;
  }

  public void bootstrap() {
    config = configuration(aggregateClass).put("schema", camelToKebab(aggregateClass.getSimpleName()));
    final var moduleBuilder = ModuleBuilder.create().install(EventxMain.MAIN_MODULES);
    moduleBuilder.bind(Vertx.class).toInstance(vertx);
    moduleBuilder.bind(JsonObject.class).toInstance(config);
    injector = Injector.of(moduleBuilder.build());
    if (Boolean.TRUE.equals(infrastructure())) {
      deployPgContainer();
    }
    this.httpClient = new AggregateHttpClient<>(
      WebClient.create(vertx, new WebClientOptions()
        .setDefaultHost(HTTP_HOST)
        .setDefaultPort(HTTP_PORT)
      ),
      aggregateClass
    );
    vertx.deployVerticle(EventxMain::new, new DeploymentOptions().setInstances(1).setConfig(config)).await().indefinitely();
    this.eventBusPoxy = new AggregateEventBusPoxy<>(vertx, aggregateClass);
  }


  public Bootstrapper<T> addModule(Module module) {
    EventxMain.MAIN_MODULES.add(module);
    return this;
  }

  public Bootstrapper<T> setRemoteHost(String host) {
    this.HTTP_HOST = host;
    return this;
  }

  public Bootstrapper<T> setRemotePort(Integer port) {
    this.HTTP_PORT = port;
    return this;
  }

  public Bootstrapper<T> setPostgres(final Boolean postgres) {
    this.postgres = postgres;
    return this;
  }

  public Boolean infrastructure() {
    return postgres;
  }


  public JsonObject configuration(Class<? extends Aggregate> aggregateClass) {
    return vertx.fileSystem().readFileBlocking(aggregateClass.getSimpleName() + ".json").toJsonObject();
  }

  private void deployRedisContainer() {
    redis = new GenericContainer(DockerImageName.parse("redis:latest"))
      .withExposedPorts(6379)
      .waitingFor(Wait.forListeningPort())
      .withNetwork(network);
    redis.start();
    config.put("redisHost", redis.getHost());
    config.put("redisPort", redis.getFirstMappedPort());
    vertx.fileSystem().writeFileBlocking(aggregateClass.getSimpleName() + ".json", Buffer.newInstance(config.toBuffer()));
    LOGGER.debug("Configuration after container bootstrap {}", config);
  }

  public void deployPgContainer() {
    postgreSQLContainer = new PostgreSQLContainer<>(POSTGRES_VERSION)
      .withNetwork(network)
      .waitingFor(Wait.forListeningPort());
    postgreSQLContainer.start();
    config.put(Constants.PG_HOST, postgreSQLContainer.getHost())
      .put(Constants.PG_PORT, postgreSQLContainer.getFirstMappedPort())
      .put(Constants.PG_USER, postgreSQLContainer.getUsername())
      .put(Constants.PG_PASSWORD, postgreSQLContainer.getPassword())
      .put(Constants.PG_DATABASE, postgreSQLContainer.getDatabaseName())
      .put(Constants.JDBC_URL, postgreSQLContainer.getJdbcUrl());
    vertx.fileSystem().writeFileBlocking(aggregateClass.getSimpleName() + ".json", Buffer.newInstance(config.toBuffer()));
    LOGGER.debug("Configuration after container bootstrap {}", config);
  }

  public void deployZookeeper() {
    zookeeperContainer = new GenericContainer<>(DockerImageName.parse(ZOOKEEPER_VERSION))
      .withNetwork(network)
      .waitingFor(Wait.forListeningPort());
    zookeeperContainer.start();
  }

  public void destroy() {
    vertx.closeAndAwait();
    if (Boolean.TRUE.equals(infrastructure())) {
      LOGGER.info(postgreSQLContainer.getLogs());
      postgreSQLContainer.stop();
      postgreSQLContainer.close();
    }
  }


}