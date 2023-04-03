package io.vertx.eventx.launcher;

import io.activej.inject.Injector;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.config.*;
import io.vertx.eventx.config.orm.ConfigurationKey;
import io.vertx.eventx.config.orm.ConfigurationQuery;
import io.vertx.eventx.config.orm.ConfigurationRecord;
import io.vertx.eventx.config.orm.ConfigurationRecordMapper;
import io.vertx.eventx.infrastructure.misc.CustomClassLoader;
import io.vertx.eventx.sql.LiquibaseHandler;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.eventx.sql.models.QueryOptions;
import io.vertx.mutiny.config.ConfigRetriever;
import io.vertx.mutiny.core.Promise;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.pgclient.pubsub.PgSubscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class ConfigLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigLauncher.class);
  public static final List<ConfigRetriever> CONFIG_RETRIEVERS = new ArrayList<>();
  public static PgSubscriber PG_SUBSCRIBER;

  private ConfigLauncher() {
  }

  public static Uni<Void> deploy(Injector injector) {
    final var repository = new Repository<>(ConfigurationRecordMapper.INSTANCE, injector.getInstance(RepositoryHandler.class));
    final var fsConfigs = CustomClassLoader.loadFromInjector(injector, FSConfig.class);
    final var configUni = new ArrayList<Uni<Void>>();
    if (!fsConfigs.isEmpty()) {
      LOGGER.info("File-System configuration detected");
      configUni.add(fsConfigurations(injector.getInstance(Vertx.class), fsConfigs));
    }
    if (CustomClassLoader.checkPresence(injector, DBConfig.class)) {
      LOGGER.info("Database configuration detected");
      configUni.add(dbConfigurations(injector.getInstance(RepositoryHandler.class), repository));
    }
    if (!configUni.isEmpty()) {
      return Uni.join().all(configUni).andFailFast().replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }

  private static Uni<Void> fsConfigurations(Vertx vertx, List<FSConfig> fsConfigs) {
    final var promiseMap = fsConfigs.stream().map(cfg -> Map.entry(cfg.name(), Promise.promise()))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    CONFIG_RETRIEVERS.addAll(
      fsConfigs.stream()
        .map(fileConfiguration -> ConfigurationHandler.configure(
            vertx,
            fileConfiguration.name(),
            newConfiguration -> {
              try {
                LOGGER.info("Adding " + fileConfiguration.name() + " to cache");
                FsConfigCache.put(fileConfiguration.name(), newConfiguration);
                final var promise = promiseMap.get(fileConfiguration.name());
                if (promise != null) {
                  promise.complete();
                }
              } catch (Exception e) {
                LOGGER.error("unable to consume configuration", e);
                final var promise = promiseMap.get(fileConfiguration.name());
                if (promise != null) {
                  promise.complete();
                }
              }
            }
          )
        )
        .toList()
    );
    return Uni.join().all(promiseMap.values().stream().map(Promise::future).toList())
      .andFailFast().invoke(avoid -> promiseMap.clear())
      .replaceWithVoid();
  }

  private static Uni<Void> dbConfigurations(RepositoryHandler repositoryHandler, Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> repository) {
    PG_SUBSCRIBER = PgSubscriber.subscriber(repositoryHandler.vertx(),
      RepositoryHandler.connectionOptions(repositoryHandler.configuration())
    );
    PG_SUBSCRIBER.reconnectPolicy(integer -> 0L);
    final var pgChannel = PG_SUBSCRIBER.channel("configuration_channel");
    pgChannel.handler(id -> {
          final ConfigurationKey key = configurationKey(id);
          LOGGER.info("Updating configuration -> " + key);
          repository.selectByKey(key)
            .onItemOrFailure().transform((item, failure) -> handleMessage(key, item, failure))
            .subscribe()
            .with(
              item -> LOGGER.info("Configuration updated -> " + key),
              throwable -> LOGGER.error("Unable to synchronize configuration -> " + key, throwable)
            );
        }
      )
      .endHandler(() -> subscriptionStopped(PG_SUBSCRIBER))
      .subscribeHandler(ConfigLauncher::handleSubscription)
      .exceptionHandler(ConfigLauncher::handleSubscriberError);
    return liquibase(repositoryHandler)
      .call(avoid -> warmCaches(repository))
      .call(avoid -> PG_SUBSCRIBER.connect())
      .replaceWithVoid();
  }

  private static Object handleMessage(final ConfigurationKey key, final ConfigurationRecord updatedConfig, final Throwable failure) {
    final var currentCachedEntry = DbConfigCache.get(parseKey(updatedConfig));
    // if updated config active
    ConfigurationEntry currentConfigState = null;
    if (currentCachedEntry != null) {
      currentConfigState = mapData(currentCachedEntry, updatedConfig.tClass());
    }
    // this means that config was no longer in  the db.
    if (failure != null) {
      if (currentConfigState != null && currentConfigState.revision().equals(updatedConfig.revision())) {
        LOGGER.info("Deleting configuration -> " + key);
        DbConfigCache.delete(parseKey(key));
      }
    } else {
      if (updatedConfig.active()) {
        LOGGER.info("Loading configuration -> " + key);
        DbConfigCache.put(parseKey(key), updatedConfig.data());
      } else if (currentConfigState != null && currentConfigState.revision().equals(updatedConfig.revision())) {
        LOGGER.info("Deactivating configuration -> " + key);
        DbConfigCache.delete(parseKey(key));
      }
    }
    return updatedConfig;
  }

  private static String parseKey(ConfigurationKey key) {
    return new StringJoiner("::")
      .add(key.name())
      .add(key.tenantId())
      .add(key.tClass())
      .toString();
  }

  public static String parseKey(ConfigurationRecord record) {
    return new StringJoiner("::")
      .add(record.name())
      .add(record.baseRecord().tenantId())
      .add(record.tClass())
      .toString();
  }

  private static ConfigurationKey configurationKey(final String id) {
    LOGGER.info("Parsing configuration channel message -> " + id);
    final var splitId = id.split("::");
    final var name = splitId[0];
    final var tClass = splitId[1];
    final var tenant = splitId[2];
    final var revision = Integer.parseInt(splitId[3]);
    return new ConfigurationKey(name, tClass, revision, tenant);
  }

  private static Uni<Void> warmCaches(final Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> repository) {
    return repository.stream(configurationRecord -> {
        final var key = new ConfigurationKey(configurationRecord.name(), configurationRecord.tClass(), configurationRecord.revision(), configurationRecord.baseRecord().tenantId());
        handleMessage(key, configurationRecord, null);
      }
      ,
      new ConfigurationQuery(
        null,
        null,
        QueryOptions.simple()
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
  }

  private static ConfigurationEntry mapData(final ConfigurationRecord item) {
    try {
      return (ConfigurationEntry) item.data().mapTo(Class.forName(item.tClass()));
    } catch (ClassNotFoundException e) {
      LOGGER.error("Unable to cast configuration using class for name");
      throw new IllegalArgumentException(e);
    }
  }

  private static ConfigurationEntry mapData(final JsonObject item, final String tClass) {
    try {
      return (ConfigurationEntry) item.mapTo(Class.forName(tClass));
    } catch (ClassNotFoundException e) {
      LOGGER.error("Unable to cast configuration using class for name");
      throw new IllegalArgumentException(e);
    }
  }

  public static Uni<Void> close() {
    if (!CONFIG_RETRIEVERS.isEmpty()) {
      CONFIG_RETRIEVERS.forEach(ConfigRetriever::close);
      return PG_SUBSCRIBER.close();
    }
    return Uni.createFrom().voidItem();
  }
}