package io.vertx.eventx.config.orm;

import io.vertx.core.shareddata.Shareable;
import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record ConfigurationKey(
  String name,
  String tClass,
  String tenant
) implements RepositoryRecordKey, Shareable {

}
