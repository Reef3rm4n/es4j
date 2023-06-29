package io.es4j.infra.pg.models;

import io.es4j.sql.models.RepositoryRecordKey;

public record ProjectionHistoryKey(
  String entityId,
  String projectionClass,
  String tenant
) implements RepositoryRecordKey {

}
