package io.vertx.eventx.launcher;

import io.activej.inject.Injector;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.UniHelper;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.config.ConfigurationHandler;
import io.vertx.eventx.config.DBConfig;
import io.vertx.eventx.config.FSConfig;
import io.vertx.eventx.config.FsConfigCache;
import io.vertx.eventx.config.orm.ConfigurationKey;
import io.vertx.eventx.config.orm.ConfigurationQuery;
import io.vertx.eventx.config.orm.ConfigurationRecord;
import io.vertx.eventx.config.orm.ConfigurationRecordMapper;
import io.vertx.eventx.sql.LiquibaseHandler;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.mutiny.config.ConfigRetriever;
import io.vertx.mutiny.core.Promise;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.pgclient.pubsub.PgSubscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ConfigLauncher {


  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigLauncher.class);
  public List<ConfigRetriever> listeners;
  public PgSubscriber pgSubscriber;

  private ConfigLauncher() {
  }

  public static final ConfigLauncher INSTANCE = new ConfigLauncher();

  public Uni<Void> deploy(Injector injector) {
    final var repository = new Repository<>(ConfigurationRecordMapper.INSTANCE, injector.getInstance(RepositoryHandler.class));
    final var fsConfigs = CustomClassLoader.loadFromInjector(injector, FSConfig.class);
    if (!fsConfigs.isEmpty()) {
      LOGGER.info("File-System configuration detected");
      return fsConfigurations(injector.getInstance(Vertx.class), fsConfigs);
    }
    if (CustomClassLoader.checkPresence(injector, DBConfig.class)) {
      LOGGER.info("Database configuration detected");
      return dbConfigurations(injector, injector.getInstance(RepositoryHandler.class), repository)
        .replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> fsConfigurations(Vertx vertx, List<FSConfig> fsConfigs) {
    final var promiseMap = fsConfigs.stream().map(cfg -> Map.entry(cfg.name(), Promise.promise()))
      .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
    this.listeners = fsConfigs.stream().map(
      cfg -> ConfigurationHandler.configure(vertx, cfg.name(),
        kubeConfigChange -> {
          try {
            LOGGER.info("Adding " + cfg.name() + " to cache");
            FsConfigCache.put(cfg.name(), kubeConfigChange);
            final var promise = promiseMap.get(cfg.name());
            if (promise != null) {
              promise.complete();
            }
          } catch (Exception e) {
            LOGGER.error("unable to consume configuration", e);
            final var promise = promiseMap.get(cfg.name());
            if (promise != null) {
              promise.complete();
            }
          }
        }
      )
    ).toList();
    return Uni.join().all(promiseMap.values().stream().map(Promise::future).toList())
      .andFailFast().invoke(avoid -> promiseMap.clear())
      .replaceWithVoid();
  }

  private Uni<Injector> dbConfigurations(Injector injector, RepositoryHandler repositoryHandler, Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> repository) {
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
      .subscribeHandler(ConfigLauncher::handleSubscription)
      .exceptionHandler(ConfigLauncher::handleSubscriberError);
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
