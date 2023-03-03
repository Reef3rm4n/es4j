package io.vertx.eventx.infrastructure.pg.models;

import io.vertx.eventx.sql.models.RepositoryRecordKey;

public record ProjectionHistoryKey(
  String entityId,
  String projectionClass,
  String tenant
) implements RepositoryRecordKey {

}
