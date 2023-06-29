package io.es4j.config;


import com.google.auto.service.AutoService;
import io.es4j.Aggregate;
import io.es4j.config.orm.ConfigurationKey;
import io.es4j.config.orm.ConfigurationQuery;
import io.es4j.config.orm.ConfigurationRecord;
import io.es4j.config.orm.ConfigurationRecordMapper;
import io.es4j.infrastructure.AggregateServices;
import io.es4j.sql.LiquibaseHandler;
import io.es4j.sql.Repository;
import io.es4j.sql.RepositoryHandler;
import io.es4j.sql.models.QueryOptions;
import io.smallrye.mutiny.Uni;

import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.pgclient.pubsub.PgSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.es4j.core.CommandHandler.camelToKebab;


@AutoService(AggregateServices.class)
public class DatabaseConfigurationService implements AggregateServices {

  private static final AtomicBoolean LIQUIBASE = new AtomicBoolean(false);

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseConfigurationService.class);
  public static PgSubscriber PG_SUBSCRIBER;
  private Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> repository;

  public Uni<Void> start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration) {
    final var configUni = new ArrayList<Uni<Void>>();
    final var repositoryHandler = RepositoryHandler.leasePool(configuration, vertx);
    this.repository = new Repository<>(ConfigurationRecordMapper.INSTANCE, repositoryHandler);
    configUni.add(dbConfigurations(repositoryHandler, repository));
    return Uni.join().all(configUni).andFailFast().replaceWithVoid();
  }

  @Override
  public Uni<Void> stop() {
    return PG_SUBSCRIBER.close()
      .flatMap(avoid -> repository.repositoryHandler().close());
  }

  private static Uni<Void> dbConfigurations(RepositoryHandler repositoryHandler, Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> repository) {
    PG_SUBSCRIBER = PgSubscriber.subscriber(repositoryHandler.vertx(),
      RepositoryHandler.connectionOptions(repositoryHandler.configuration())
    );
    PG_SUBSCRIBER.reconnectPolicy(integer -> 0L);
    final var pgChannel = PG_SUBSCRIBER.channel(camelToKebab(repositoryHandler.configuration().getString("schema")) + "_configuration_channel");
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
      .subscribeHandler(DatabaseConfigurationService::handleSubscription)
      .exceptionHandler(DatabaseConfigurationService::handleSubscriberError);
    return liquibase(repositoryHandler)
      .call(avoid -> warmCaches(repository))
      .call(avoid -> PG_SUBSCRIBER.connect())
      .replaceWithVoid();
  }

  private static Object handleMessage(final ConfigurationKey key, final ConfigurationRecord updatedConfig, final Throwable failure) {
    final var currentCachedEntry = DatabaseConfigurationCache.get(parseKey(updatedConfig));
    // if updated config active
    DatabaseConfiguration currentConfigState = null;
    if (currentCachedEntry != null) {
      currentConfigState = mapData(currentCachedEntry, updatedConfig.tClass());
    }
    // this means that config was no longer in  the db.
    if (failure != null) {
      if (currentConfigState != null && currentConfigState.revision().equals(updatedConfig.revision())) {
        LOGGER.info("Deleting configuration {}", JsonObject.mapFrom(key).encodePrettily());
        DatabaseConfigurationCache.invalidate(parseKey(key));
      }
    } else {
      if (updatedConfig.active()) {
        LOGGER.info("Loading configuration {}", JsonObject.mapFrom(key).encodePrettily());
        DatabaseConfigurationCache.put(parseKey(key), updatedConfig.data());
      } else if (currentConfigState != null && currentConfigState.revision().equals(updatedConfig.revision())) {
        LOGGER.info("Deactivating configuration {} ", JsonObject.mapFrom(key));
        DatabaseConfigurationCache.invalidate(parseKey(key));
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
      .add(record.baseRecord().tenant())
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
        final var key = new ConfigurationKey(configurationRecord.tClass(), configurationRecord.revision(), configurationRecord.baseRecord().tenant());
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
    if (LIQUIBASE.compareAndSet(false, true)) {
      return LiquibaseHandler.liquibaseString(
        repositoryHandler,
        "config.xml",
        Map.of("schema", repositoryHandler.configuration().getString("schema"))
      );
    }
    return Uni.createFrom().voidItem();
  }

  private static void handleSubscriberError(final Throwable throwable) {
    LOGGER.error("PgSubscriber had to drop throwable", throwable);
  }

  private static void handleSubscription() {
    LOGGER.info("subscription started");
  }

  private static void subscriptionStopped(final PgSubscriber pgSubscriber) {
    LOGGER.info("subscription stopped");
  }

  private static DatabaseConfiguration mapData(final ConfigurationRecord item) {
    try {
      return (DatabaseConfiguration) item.data().mapTo(Class.forName(item.tClass()));
    } catch (ClassNotFoundException e) {
      LOGGER.error("Unable to cast configuration using class for fileName");
      throw new IllegalArgumentException(e);
    }
  }

  private static DatabaseConfiguration mapData(final JsonObject item, final String tClass) {
    try {
      return (DatabaseConfiguration) item.mapTo(Class.forName(tClass));
    } catch (ClassNotFoundException e) {
      LOGGER.error("Unable to cast configuration using class for {} to class {}", item.encodePrettily(), tClass, e);
      throw new IllegalArgumentException(e);
    }
  }

}
