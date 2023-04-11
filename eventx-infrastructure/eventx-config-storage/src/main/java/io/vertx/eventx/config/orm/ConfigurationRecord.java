package io.vertx.eventx.config.orm;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.sql.models.BaseRecord;
import io.vertx.eventx.sql.models.RepositoryRecord;


@RecordBuilder
public record ConfigurationRecord(
  String name,
  String description,
  Integer revision,
  String tClass,
  JsonObject data,
  Boolean active,
  BaseRecord baseRecord
) implements RepositoryRecord<ConfigurationRecord> {
  @Override
  public ConfigurationRecord with(BaseRecord baseRecord) {
    return new ConfigurationRecord(name, description, revision, tClass, data, active,baseRecord);
  }



}
