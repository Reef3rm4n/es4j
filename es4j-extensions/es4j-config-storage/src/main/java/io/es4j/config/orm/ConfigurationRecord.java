package io.es4j.config.orm;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;
import io.es4j.sql.models.BaseRecord;
import io.es4j.sql.models.RepositoryRecord;


@RecordBuilder
public record ConfigurationRecord(
  String description,
  Integer revision,
  String tClass,
  JsonObject data,
  Boolean active,
  BaseRecord baseRecord
) implements RepositoryRecord<ConfigurationRecord> {
  @Override
  public ConfigurationRecord with(BaseRecord baseRecord) {
    return new ConfigurationRecord(description, revision, tClass, data, active, baseRecord);
  }


}
