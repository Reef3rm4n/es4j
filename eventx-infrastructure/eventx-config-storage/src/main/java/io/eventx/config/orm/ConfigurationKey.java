package io.eventx.config.orm;

import io.vertx.core.shareddata.Shareable;
import io.eventx.sql.models.RepositoryRecordKey;

public record ConfigurationKey(
  String name,
  String tClass,
  Integer revision,
  String tenant
) implements RepositoryRecordKey, Shareable {

}
