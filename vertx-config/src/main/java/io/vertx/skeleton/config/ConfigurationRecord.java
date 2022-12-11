package io.vertx.skeleton.config;

import io.vertx.skeleton.models.PersistedRecord;
import io.vertx.skeleton.models.RepositoryRecord;
import io.vertx.core.json.JsonObject;

public record ConfigurationRecord(
  String name,
  String tClass,
  JsonObject data,
  PersistedRecord persistedRecord
) implements RepositoryRecord<ConfigurationRecord> {

  @Override
  public ConfigurationRecord with(final PersistedRecord persistedRecord) {
    return new ConfigurationRecord(name, tClass, data, persistedRecord);
  }

}
