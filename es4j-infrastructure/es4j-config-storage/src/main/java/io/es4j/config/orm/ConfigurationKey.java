package io.es4j.config.orm;

import io.vertx.core.shareddata.Shareable;
import io.es4j.sql.models.RepositoryRecordKey;

public record ConfigurationKey(
  String tClass,
  Integer revision,
  String tenant
) implements RepositoryRecordKey, Shareable {

}
