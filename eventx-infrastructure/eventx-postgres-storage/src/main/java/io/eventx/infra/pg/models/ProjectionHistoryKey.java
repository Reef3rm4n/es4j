package io.eventx.infra.pg.models;

import io.eventx.sql.models.RepositoryRecordKey;

public record ProjectionHistoryKey(
  String entityId,
  String projectionClass,
  String tenant
) implements RepositoryRecordKey {

}
