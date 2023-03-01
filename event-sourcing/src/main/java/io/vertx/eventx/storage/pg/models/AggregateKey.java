package io.vertx.eventx.storage.pg.models;

import io.vertx.core.shareddata.Shareable;
import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record AggregateKey(
  String aggregateId,
  String tenantId
) implements RepositoryRecordKey, Shareable {
}
