package io.eventx.infra.pg.models;

import io.eventx.sql.models.RepositoryRecordKey;
import io.vertx.core.shareddata.Shareable;

public record AggregateRecordKey(
  String aggregateId,
  String tenantId
) implements RepositoryRecordKey, Shareable {
}
