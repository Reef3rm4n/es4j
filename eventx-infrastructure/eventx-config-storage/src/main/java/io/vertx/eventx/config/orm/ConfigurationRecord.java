package io.vertx.eventx.config.orm;

import io.vertx.core.json.JsonObject;
import io.vertx.eventx.sql.models.BaseRecord;
import io.vertx.eventx.sql.models.RepositoryRecord;

public record ConfigurationRecord(
  String name,
  String tClass,
  JsonObject data,
  BaseRecord baseRecord
) implements RepositoryRecord<ConfigurationRecord> {
  @Override
  public ConfigurationRecord with(BaseRecord baseRecord) {
    return new ConfigurationRecord(name, tClass, data, baseRecord);
  }
}
