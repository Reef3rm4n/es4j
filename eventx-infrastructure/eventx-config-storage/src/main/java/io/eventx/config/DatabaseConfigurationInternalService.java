package io.eventx.config;

import io.eventx.config.orm.ConfigurationKey;
import io.eventx.config.orm.ConfigurationQuery;
import io.eventx.config.orm.ConfigurationRecord;
import io.eventx.config.orm.ConfigurationRecordMapper;
import io.eventx.sql.models.QueryOptions;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.eventx.sql.Repository;
import io.eventx.sql.RepositoryHandler;
import io.eventx.sql.models.BaseRecord;
import io.vertx.mutiny.core.Vertx;

import java.util.*;

class DatabaseConfigurationInternalService<T extends DatabaseConfiguration> {
  private final Vertx vertx;
  private final Class<T> tClass;
  private final Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> repository;

  public DatabaseConfigurationInternalService(
    final Class<T> tClass,
    final RepositoryHandler repositoryHandler
  ) {
    this.tClass = tClass;
    this.vertx = repositoryHandler.vertx();
    this.repository = new Repository<>(ConfigurationRecordMapper.INSTANCE, repositoryHandler);
  }

  public Optional<T> fetch(String tenant) {
    final var data = DatabaseConfigurationCache.get(parseKey(tenant, tClass));
    if (Objects.nonNull(data)) {
      return Optional.of(data.mapTo(tClass));
    }
    return Optional.empty();
  }

  public Uni<T> fetchThrough(Integer revision, String tenant) {
    return repository.selectByKey(new ConfigurationKey(tClass.getName(), revision, tenant))
      .map(cfgRec -> cfgRec.data().mapTo(tClass));
  }

  public String parseKey(String tenant, Class<T> tClass) {
    return new StringJoiner("::")
      .add(tenant)
      .add(tClass.getName())
      .toString();
  }

  public Uni<T> add(T data) {
    return repository.insert(this.map(data))
      .map(configuration -> configuration.data().mapTo(tClass));
  }

  public Uni<Void> addAll(List<? extends DatabaseConfiguration> configurationEntries) {
    final var entries = configurationEntries.stream().map(this::map).toList();
    return repository.insertBatch(entries);
  }

  private ConfigurationRecord map(DatabaseConfiguration data) {
    return new ConfigurationRecord(
      data.description(),
      data.revision(),
      tClass.getName(),
      JsonObject.mapFrom(data),
      data.active(),
      BaseRecord.newRecord(data.tenant())
    );
  }

  public Uni<Void> updateAll(List<? extends DatabaseConfiguration> configurationEntries) {
    final var entries = configurationEntries.stream().map(this::map).toList();
    return repository.updateByKeyBatch(entries);
  }

  public Uni<T> update(T value) {
    return repository.updateByKey(this.map(value))
      .map(configuration -> configuration.data().mapTo(tClass));
  }

  public Uni<Void> delete(Integer revision, String tenant) {
    return repository.deleteByKey(new ConfigurationKey(tClass.getName(), revision, tenant));
  }

  public Set<Map.Entry<ConfigurationKey, T>> entrySet() {
    return vertx.getDelegate().sharedData().<ConfigurationKey, T>getLocalMap(ConfigurationRecord.class.getName()).entrySet();
  }

  public Set<ConfigurationKey> keySet() {
    return vertx.getDelegate().sharedData().<ConfigurationKey, T>getLocalMap(ConfigurationRecord.class.getName()).keySet();
  }

  public Uni<List<T>> query(QueryOptions queryOptions) {
    final var query = new ConfigurationQuery(List.of(tClass.getName()), queryOptions);
    return repository.query(query).map(configs -> configs.stream().map(cfg -> cfg.data().mapTo(tClass)).toList());
  }
}
