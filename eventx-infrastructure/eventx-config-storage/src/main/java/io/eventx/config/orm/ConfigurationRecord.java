package io.eventx.config.orm;

import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.json.JsonObject;
import io.eventx.sql.models.BaseRecord;
import io.eventx.sql.models.RepositoryRecord;


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
