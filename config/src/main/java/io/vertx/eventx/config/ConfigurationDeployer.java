package io.vertx.eventx.config;

import io.activej.inject.Injector;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.config.orm.ConfigurationKey;
import io.vertx.eventx.config.orm.ConfigurationQuery;
import io.vertx.eventx.config.orm.ConfigurationRecord;
import io.vertx.eventx.config.orm.ConfigurationRecordMapper;
import io.vertx.eventx.sql.LiquibaseHandler;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.mutiny.config.ConfigRetriever;
import io.vertx.mutiny.pgclient.pubsub.PgSubscriber;
import io.vertx.eventx.common.CustomClassLoader;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConfigurationDeployer {


  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationDeployer.class);
  public List<ConfigRetriever> listeners;
  public PgSubscriber pgSubscriber;


  public Uni<Injector> deploy(Injector injector, RepositoryHandler repositoryHandler) {
    final var repository = new Repository<>(ConfigurationRecordMapper.INSTANCE, repositoryHandler);
    final var kubernetesConfiguration = CustomClassLoader.loadFromInjector(injector, KubernetesConfiguration.class);
    if (!kubernetesConfiguration.isEmpty()) {
      bootstrapKubernetesBasedConfiguration(repositoryHandler, kubernetesConfiguration);
    }
    if (CustomClassLoader.checkPresence(injector, Configuration.class)) {
      return bootstrapRepositoryBasedConfiguration(injector, repositoryHandler, repository);
    }
    if (CustomClassLoader.checkPresence(injector, KubernetesSecret.class)) {

    }
    return Uni.createFrom().item(injector);
  }

  private void bootstrapKubernetesBasedConfiguration(RepositoryHandler repositoryHandler, List<KubernetesConfiguration> kubernetesConfiguration) {
    this.listeners = kubernetesConfiguration.stream().map(
      cfg -> {
        LOGGER.info("Bootstrapping kubernetes configmap -> " + cfg.mapName);
        return ConfigurationHandler.configure(repositoryHandler.vertx(), cfg.mapName,
          kubeConfigChange -> {
            final var tenantBasedConfiguration = kubeConfigChange.getMap().entrySet().stream()
              .map(entry -> {
                LOGGER.info("Updating kubernetes configmap for tenant -> " + entry.getKey());
                final var tenant = entry.getKey();
                final var json = JsonObject.mapFrom(entry.getValue());
                return Map.entry(tenant, json);
              })
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            repositoryHandler.vertx().getDelegate().sharedData().getLocalMap(ConfigurationRecord.class.getName())
              .putAll(tenantBasedConfiguration);
          }
        );
      }
    ).toList();
  }

  private Uni<Injector> bootstrapRepositoryBasedConfiguration(Injector injector, RepositoryHandler repositoryHandler, Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> repository) {
    this.pgSubscriber = PgSubscriber.subscriber(repositoryHandler.vertx(),
      RepositoryHandler.connectionOptions(repositoryHandler.configuration())
    );
    pgSubscriber.reconnectPolicy(integer -> 0L);
    final var pgChannel = pgSubscriber.channel("configuration_channel");
    pgChannel.handler(id -> {
          final ConfigurationKey key = configurationKey(id);
          LOGGER.info("Updating configuration -> " + key);
          repository.selectByKey(key)
            .onItemOrFailure().transform((item, failure) -> handleMessage(repository, key, item, failure))
            .subscribe()
            .with(
              item -> LOGGER.info("Configuration updated -> " + key),
              throwable -> LOGGER.error("Unable to synchronize configuration -> " + key, throwable)
            );
        }
      )
      .endHandler(() -> subscriptionStopped(pgSubscriber))
      .subscribeHandler(ConfigurationDeployer::handleSubscription)
      .exceptionHandler(ConfigurationDeployer::handleSubscriberError);
    return liquibase(repositoryHandler)
      .call(avoid -> warmCaches(repositoryHandler, repository))
      .call(avoid -> pgSubscriber.connect())
      .replaceWith(injector);
  }

  private static Object handleMessage(final Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> repository, final ConfigurationKey key, final ConfigurationRecord item, final Throwable failure) {
    if (failure != null) {
      LOGGER.info("Deleting configuration from cache -> " + key);
      return repository.repositoryHandler().vertx()
        .sharedData().getLocalMap(ConfigurationRecord.class.getName())
        .remove(key);
    } else {
      LOGGER.info("Loading new configuration into cache -> " + key);
      return repository.repositoryHandler().vertx()
        .sharedData().getLocalMap(ConfigurationRecord.class.getName())
        .put(key, mapData(item));
    }
  }


  private static ConfigurationKey configurationKey(final String id) {
    LOGGER.info("Parsing configuration channel message -> " + id);
    final var splittedId = id.split("::");
    final var name = splittedId[0];
    final var tClass = splittedId[1];
    final var tenant = splittedId[2];
    return new ConfigurationKey(name, tClass, tenant);
  }

  private static Uni<Void> warmCaches(final RepositoryHandler repositoryHandler, final Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> repository) {
    return repository.stream(configurationRecord -> {
        final var cache = repositoryHandler.vertx().sharedData().<ConfigurationKey, Object>getLocalMap(ConfigurationRecord.class.getName());
        final var key = new ConfigurationKey(configurationRecord.name(), configurationRecord.tClass(), configurationRecord.baseRecord().tenantId());
        LOGGER.info("Loading configuration into cache -> " + key);
        cache.put(key, mapData(configurationRecord));
      }
      ,
      new ConfigurationQuery(
        null,
        null,
        null
      )
    );
  }

  private static Uni<Void> liquibase(final RepositoryHandler repositoryHandler) {
    return LiquibaseHandler.liquibaseString(
      repositoryHandler,
      "config.xml",
      Map.of("schema", repositoryHandler.configuration().getString("schema"))
    );
  }

  private static void handleSubscriberError(final Throwable throwable) {
    LOGGER.error("PgSubscriber had to drop throwable", throwable);
  }

  private static void handleSubscription() {
    LOGGER.info("PgSubscribed to channel");
  }

  private static void subscriptionStopped(final PgSubscriber pgSubscriber) {
    LOGGER.info("Pg subscription dropped");
//    pgSubscriber.connectAndForget();
  }

  private static Object mapData(final ConfigurationRecord item) {
    try {
      return item.data().mapTo(Class.forName(item.tClass()));
    } catch (ClassNotFoundException e) {
      LOGGER.error("Unable to cast configuration using class for name");
      throw new IllegalArgumentException(e);
    }
  }

  public Uni<Void> close() {
    if (listeners != null && !listeners.isEmpty()) {
      listeners.forEach(ConfigRetriever::close);
    }
    return pgSubscriber.close();
  }
}
