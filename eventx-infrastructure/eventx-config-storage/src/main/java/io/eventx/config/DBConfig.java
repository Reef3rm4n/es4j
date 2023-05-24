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

public class DBConfig<T extends Configuration> {
  private final Vertx vertx;
  private final Class<T> tClass;
  private final Repository<ConfigurationKey, ConfigurationRecord, ConfigurationQuery> repository;

  public DBConfig(
    final Class<T> tClass,
    final RepositoryHandler repositoryHandler
  ) {
    this.tClass = tClass;
    this.vertx = repositoryHandler.vertx();
    this.repository = new Repository<>(ConfigurationRecordMapper.INSTANCE, repositoryHandler);
  }

  public T fetch(String tenant) {
    return Objects.requireNonNull(
      DbConfigCache.get(parseKey(tenant, tClass)), "configuration not found"
    ).mapTo(tClass);
  }

  public Uni<T> fetchThrough(String name, Integer revision, String tenant) {
    return repository.selectByKey(new ConfigurationKey(name, tClass.getName(), revision, tenant))
      .map(cfgRec -> cfgRec.data().mapTo(tClass));
  }

  public String parseKey(String tenant, Class<T> tClass) {
    return new StringJoiner("::")
      .add(tenant)
      .add(tClass.getName())
      .toString();
  }

  public Uni<T> add(T data) {
    return repository.insert(DBConfig.map(data))
      .map(configuration -> configuration.data().mapTo(tClass));
  }

  public Uni<Void> addAll(List<? extends Configuration> configurationEntries) {
    final var entries = configurationEntries.stream().map(DBConfig::map).toList();
    return repository.insertBatch(entries);
  }

  private static ConfigurationRecord map(Configuration data) {
    return new ConfigurationRecord(
      data.name(),
      data.description(),
      data.revision(),
      data.getClass().getName(),
      JsonObject.mapFrom(data),
      data.active(),
      BaseRecord.newRecord(data.tenant())
    );
  }

  public Uni<Void> updateAll(List<? extends Configuration> configurationEntries) {
    final var entries = configurationEntries.stream().map(DBConfig::map).toList();
    return repository.updateByKeyBatch(entries);
  }

  public Uni<T> update(T value) {
    return repository.updateByKey(DBConfig.map(value))
      .map(configuration -> configuration.data().mapTo(tClass));
  }

  public Uni<Void> delete(String name, Integer revision, String tenant) {
    return repository.deleteByKey(new ConfigurationKey(name, tClass.getName(), revision, tenant));
  }

  public Set<Map.Entry<ConfigurationKey, T>> entrySet() {
    return vertx.getDelegate().sharedData().<ConfigurationKey, T>getLocalMap(ConfigurationRecord.class.getName()).entrySet();
  }

  public Set<ConfigurationKey> keySet() {
    return vertx.getDelegate().sharedData().<ConfigurationKey, T>getLocalMap(ConfigurationRecord.class.getName()).keySet();
  }

  public Uni<List<T>> query(List<String> names, QueryOptions queryOptions) {
    final var query = new ConfigurationQuery(names, List.of(tClass.getName()), queryOptions);
    return repository.query(query).map(configs -> configs.stream().map(cfg -> cfg.data().mapTo(tClass)).toList());
  }
}
