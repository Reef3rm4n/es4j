package io.vertx.eventx.config;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.config.orm.ConfigurationKey;
import io.vertx.eventx.config.orm.ConfigurationQuery;
import io.vertx.eventx.config.orm.ConfigurationRecord;
import io.vertx.eventx.config.orm.ConfigurationRecordMapper;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.eventx.sql.models.BaseRecord;
import io.vertx.eventx.sql.models.QueryOptions;
import io.vertx.mutiny.core.Vertx;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class DBConfig<T extends ConfigurationEntry> {
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

  public T fetch(String name, String tenant) {
    return DbConfigCache.get(parseKey(name, tenant, tClass)).mapTo(tClass);
  }

  public Uni<T> fetchThrough(String name, Integer revision, String tenant) {
    return repository.selectByKey(new ConfigurationKey(name, tClass.getName(), revision, tenant))
      .map(cfgRec -> cfgRec.data().mapTo(tClass));
  }
  public String parseKey(String name, String tenant, Class<T> tClass) {
    return new StringJoiner("::").add(name).add(tenant).add(tClass.getName()).toString();
  }

  public Uni<T> add(T data) {
    return repository.insert(DBConfig.map(data))
      .map(configuration -> configuration.data().mapTo(tClass));
  }

  public Uni<Void> addAll(List<? extends ConfigurationEntry> configurationEntries) {
    final var entries = configurationEntries.stream().map(DBConfig::map).toList();
    return repository.insertBatch(entries);
  }

  private static ConfigurationRecord map(ConfigurationEntry data) {
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

  public Uni<Void> updateAll(List<? extends ConfigurationEntry> configurationEntries) {
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
