package io.eventx.launcher;

import io.activej.inject.Injector;
import io.eventx.config.*;
import io.eventx.config.orm.ConfigurationKey;
import io.eventx.config.orm.ConfigurationQuery;
import io.eventx.config.orm.ConfigurationRecord;
import io.eventx.config.orm.ConfigurationRecordMapper;
import io.eventx.core.CommandHandler;
import io.eventx.sql.LiquibaseHandler;
import io.eventx.sql.models.QueryOptions;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.eventx.infrastructure.misc.Loader;
import io.eventx.sql.Repository;
import io.eventx.sql.RepositoryHandler;
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

  public static Uni<Void> addConfigurations(Injector injector) {
    final var repository = new Repository<>(ConfigurationRecordMapper.INSTANCE, RepositoryHandler.leasePool(injector.getInstance(JsonObject.class), injector.getInstance(Vertx.class)));
    final var configUni = new ArrayList<Uni<Void>>();
    if (Loader.checkPresence(injector, FileBusinessRule.class)) {
      LOGGER.info("File-System configuration detected");
      configUni.add(fsConfigurations(injector));
    }
    if (Loader.checkPresence(injector, DatabaseBusinessRule.class)) {
      LOGGER.info("Database configuration detected");
      configUni.add(dbConfigurations(injector.getInstance(RepositoryHandler.class), repository));
    }
    if (!configUni.isEmpty()) {
      return Uni.join().all(configUni).andFailFast().replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }

  private static Uni<Void> fsConfigurations(Injector injector) {
    final var vertx = injector.getInstance(Vertx.class);
    final var fsConfigs = Loader.loadFromInjectorClass(injector, FileBusinessRule.class);
    final var promiseMap = fsConfigs.stream().map(cfg -> Map.entry(cfg.fileName(), Promise.promise()))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    CONFIG_RETRIEVERS.addAll(
      fsConfigs.stream()
        .map(fileConfiguration -> ConfigurationHandler.configure(
            vertx,
            fileConfiguration.fileName(),
            newConfiguration -> {
              try {
                LOGGER.info("Caching file configuration {} {}", fileConfiguration.fileName(), newConfiguration);
                FsConfigCache.put(fileConfiguration.fileName(), newConfiguration);
                final var promise = promiseMap.get(fileConfiguration.fileName());
                if (promise != null) {
                  promise.complete();
                }
              } catch (Exception e) {
                LOGGER.error("Unable to consume file configuration {} {}", fileConfiguration.fileName(), newConfiguration, e);
                final var promise = promiseMap.get(fileConfiguration.fileName());
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
    final var pgChannel = PG_SUBSCRIBER.channel(CommandHandler.camelToKebab(repositoryHandler.configuration().getString("schema")) + "_configuration_channel");
    pgChannel.handler(id -> {
          final ConfigurationKey key = configurationKey(id);
          LOGGER.info("Updating configuration {} ", JsonObject.mapFrom(key).encodePrettily());
          repository.selectByKey(key)
            .onItemOrFailure().transform((item, failure) -> handleMessage(key, item, failure))
            .subscribe()
            .with(
              item -> LOGGER.info("Configuration updated {} ", JsonObject.mapFrom(key).encodePrettily()),
              throwable -> LOGGER.error("Unable to synchronize configuration {} ", JsonObject.mapFrom(key).encodePrettily(), throwable)
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
    BusinessRule currentConfigState = null;
    if (currentCachedEntry != null) {
      currentConfigState = mapData(currentCachedEntry, updatedConfig.tClass());
    }
    // this means that config was no longer in  the db.
    if (failure != null) {
      if (currentConfigState != null && currentConfigState.revision().equals(updatedConfig.revision())) {
        LOGGER.info("Deleting configuration {}", JsonObject.mapFrom(key).encodePrettily());
        DbConfigCache.delete(parseKey(key));
      }
    } else {
      if (updatedConfig.active()) {
        LOGGER.info("Loading configuration {}", JsonObject.mapFrom(key).encodePrettily());
        DbConfigCache.put(parseKey(key), updatedConfig.data());
      } else if (currentConfigState != null && currentConfigState.revision().equals(updatedConfig.revision())) {
        LOGGER.info("Deactivating configuration {} ", JsonObject.mapFrom(key));
        DbConfigCache.delete(parseKey(key));
      }
    }
    return updatedConfig;
  }

  public static String parseKey(ConfigurationKey key) {
    return new StringJoiner("::")
      .add(key.tenantId())
      .add(key.tClass())
      .toString();
  }

  public static String parseKey(ConfigurationRecord record) {
    return new StringJoiner("::")
      .add(record.baseRecord().tenantId())
      .add(record.tClass())
      .toString();
  }

  private static ConfigurationKey configurationKey(final String id) {
    LOGGER.debug("Parsing channel message {}", id);
    final var splitId = id.split("::");
    final var tClass = splitId[0];
    final var tenant = splitId[1];
    final var revision = Integer.parseInt(splitId[2]);
    return new ConfigurationKey(tClass, revision, tenant);
  }

  private static Uni<Void> warmCaches(final Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> repository) {
    return repository.stream(configurationRecord -> {
        final var key = new ConfigurationKey(configurationRecord.tClass(), configurationRecord.revision(), configurationRecord.baseRecord().tenantId());
        handleMessage(key, configurationRecord, null);
      }
      ,
      new ConfigurationQuery(
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

  private static BusinessRule mapData(final ConfigurationRecord item) {
    try {
      return (BusinessRule) item.data().mapTo(Class.forName(item.tClass()));
    } catch (ClassNotFoundException e) {
      LOGGER.error("Unable to cast configuration using class for fileName");
      throw new IllegalArgumentException(e);
    }
  }

  private static BusinessRule mapData(final JsonObject item, final String tClass) {
    try {
      return (BusinessRule) item.mapTo(Class.forName(tClass));
    } catch (ClassNotFoundException e) {
      LOGGER.error("Unable to cast configuration using class for {} to class {}", item.encodePrettily(), tClass, e);
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
