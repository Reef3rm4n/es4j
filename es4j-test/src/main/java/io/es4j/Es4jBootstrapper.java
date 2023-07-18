package io.es4j;

import io.es4j.infrastructure.AggregateCache;
import io.es4j.infrastructure.EventStore;
import io.es4j.infrastructure.OffsetStore;
import io.es4j.infrastructure.cache.CaffeineAggregateCache;
import io.es4j.infrastructure.misc.Es4jServiceLoader;
import io.es4j.sql.misc.Constants;
import io.vertx.core.DeploymentOptions;
import io.es4j.infrastructure.proxy.AggregateEventBusPoxy;
import io.es4j.launcher.Es4jMain;
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

import static io.es4j.core.CommandHandler.camelToKebab;


class Es4jBootstrapper<T extends Aggregate> {
  private final String infraConfig;
  public AggregateEventBusPoxy<T> eventBusPoxy;
  public AggregateHttpClient<T> httpClient;
  private static Network network = Network.newNetwork();
  public static final String POSTGRES_VERSION = "postgres:latest";
  public static final String ZOOKEEPER_VERSION = "bitnami/zookeeper:latest";
  private final Logger LOGGER = LoggerFactory.getLogger(Es4jBootstrapper.class);
  public PostgreSQLContainer<?> postgreSQLContainer;
  public GenericContainer redis;

  public static Vertx vertx;

  public JsonObject config;
  public Boolean postgres = Boolean.parseBoolean(System.getenv().getOrDefault("POSTGRES", "false"));
  public Boolean clustered = Boolean.parseBoolean(System.getenv().getOrDefault("CLUSTERED", "false"));
  public String HTTP_HOST = System.getenv().getOrDefault("HTTP_HOST", "localhost");
  public Integer HTTP_PORT = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));
  public Class<T> aggregateClass;
  private GenericContainer zookeeperContainer;
  public AggregateCache cache;
  public EventStore eventStore;
  public OffsetStore offsetStore;

  public Es4jBootstrapper(
    Class<T> aggregateClass,
    String infraConfig
  ) {
    vertx = Vertx.vertx();
    this.infraConfig = infraConfig;
    this.aggregateClass = aggregateClass;
  }

  public void bootstrap() {
    final var deployment = new Deployment() {
      @Override
      public Class<? extends Aggregate> aggregateClass() {
        return aggregateClass;
      }
    };
    config = configuration().put("schema", camelToKebab(aggregateClass.getSimpleName()));
    if (Boolean.TRUE.equals(infrastructure())) {
      this.eventStore = Es4jServiceLoader.loadEventStore();
      deployPgContainer();
      vertx.deployVerticle(Es4jMain::new, new DeploymentOptions().setInstances(1).setConfig(config)).await().indefinitely();
      this.cache = new CaffeineAggregateCache();
      eventStore.start(deployment, vertx, config);
      this.offsetStore = Es4jServiceLoader.loadOffsetStore();
      offsetStore.start(deployment, vertx, config);
    }
    this.httpClient = new AggregateHttpClient<>(
      WebClient.create(vertx, new WebClientOptions()
        .setDefaultHost(HTTP_HOST)
        .setDefaultPort(HTTP_PORT)
      ),
      aggregateClass
    );
    this.eventBusPoxy = new AggregateEventBusPoxy<>(vertx, aggregateClass);
  }


  public Es4jBootstrapper<T> setRemoteHost(String host) {
    this.HTTP_HOST = host;
    return this;
  }

  public Es4jBootstrapper<T> setRemotePort(Integer port) {
    this.HTTP_PORT = port;
    return this;
  }

  public Es4jBootstrapper<T> setPostgres(final Boolean postgres) {
    this.postgres = postgres;
    return this;
  }

  public Boolean infrastructure() {
    return postgres;
  }


  public JsonObject configuration() {
    return vertx.fileSystem().readFileBlocking(infraConfig + ".json").toJsonObject();
  }

  private void deployRedisContainer() {
    redis = new GenericContainer(DockerImageName.parse("redis:latest"))
      .withExposedPorts(6379)
      .waitingFor(Wait.forListeningPort())
      .withNetwork(network);
    redis.start();
    config.put("redisHost", redis.getHost());
    config.put("redisPort", redis.getFirstMappedPort());
    vertx.fileSystem().writeFileBlocking(infraConfig + ".json", Buffer.newInstance(config.toBuffer()));
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
    vertx.fileSystem().writeFileBlocking(infraConfig + ".json", Buffer.newInstance(config.toBuffer()));
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
