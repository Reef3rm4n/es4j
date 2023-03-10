package io.vertx.eventx.infra.pg.models;

import io.vertx.core.shareddata.Shareable;
import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record AggregateRecordKey(
  String aggregateClass,
  String aggregateId,
  String tenantId
) implements RepositoryRecordKey, Shareable {
}
